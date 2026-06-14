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
  SimulatedBroker.submit_order()（立即收盘价成交）
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

from astock_trading.backtest.signal_analysis import signal_alpha_summary

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
    ma_idx = ma60_idx if ma60_idx > 0 else ma20_idx  # 降级用 MA20

    above_ma20 = price > ma20_idx > 0
    above_ma = price > ma_idx > 0

    # below_ma60_days（不足 60 天时，统计低于 MA20 的天数代替）
    below_days = 0
    lookback = min(20, len(closes) - 1)
    if ma_idx > 0:
        for p in reversed(closes.iloc[-lookback:].tolist()):
            if p < ma_idx:
                below_days += 1
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
            "above_ma20": above_ma20,
            "below_ma_days": below_days,
        },
    )


def _market_state_from_history_bundle(payload: dict, fallback: "MarketState") -> "MarketState":
    from astock_trading.strategy.models import MarketSignal, MarketState

    signal_value = str(payload.get("signal") or fallback.signal.value)
    signal = MarketSignal(signal_value) if signal_value in MarketSignal._value2member_map_ else fallback.signal
    return MarketState(
        signal=signal,
        multiplier=float(payload.get("multiplier", fallback.multiplier) or 0.0),
        detail=payload.get("detail") or {"source": "history_mirror"},
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


def _market_signal_value(market: object) -> str:
    return str(getattr(getattr(market, "signal", None), "value", "") or "unknown")


# ---------------------------------------------------------------------------
# BacktestEngine
# ---------------------------------------------------------------------------

@dataclass
class BacktestConfig:
    preset_name: str = "保守验证C"
    initial_cash: float = 100000.0
    adjustflag: str = "2"
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
    execute_trial_buy_market_signals: tuple[str, ...] = ()
    execute_trial_buy_routes: tuple[str, ...] = ()
    execute_watch_trial_market_signals: tuple[str, ...] = ()
    execute_watch_trial_routes: tuple[str, ...] = ()
    execute_watch_trial_pairs: tuple[str, ...] = ()
    execute_watch_trial_score_min: float = 6.0
    signal_record_limit: int | None = 50
    include_signal_alpha: bool = True
    load_financials: bool = True
    progress_log: bool = False
    use_market_bars: bool = False
    hydrate_market_bars: bool = False
    use_financial_cache: bool = False
    hydrate_financial_cache: bool = False
    financial_query_timeout_seconds: float = 45.0


@dataclass
class Position:
    code: str
    shares: int
    entry_price: float
    entry_date: str
    high_water: float
    market_reduced: bool = False  # 是否已因大盘CLEAR减过仓


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
        self._index_df: Optional[pd.DataFrame] = None  # 上证指数
        self._sorted_dates: list[str] = []
        self._portfolio_value_series: list[dict] = []
        self._trades: list[dict] = []
        self._positions: dict[str, Position] = {}
        self._cash: float = config.initial_cash
        self._weekly_buy_count: int = 0
        self._last_week: str = ""
        self._last_index_date: str = ""
        self._financial_cache: dict[str, list[dict]] = {}  # code -> point-in-time snapshots
        self._history_mirror_dates: list[str] = []
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
            "by_market_route": {},
        }
        self._requested_codes: list[str] = []
        self._loaded_codes: list[str] = []

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

        # 加载股票数据（baostock 单次最多返回 ~120 条，分多批取再合并）
        def _fetch_code(code: str) -> Optional[pd.DataFrame]:
            code_started_at = time.monotonic()
            self._log_progress("kline_start", code=code)
            cached: pd.DataFrame | None = None
            if self.cfg.use_market_bars:
                cached = self._load_market_bars_cache(code, pre_start, end_date, require_full=False)
                if cached is not None and _cache_covers_range(cached, pre_start, end_date):
                    self._log_progress(
                        "kline_cache_hit",
                        code=code,
                        rows=len(cached),
                        seconds=round(time.monotonic() - code_started_at, 2),
                    )
                    return cached
                self._log_progress(
                    "kline_cache_partial" if cached is not None and not cached.empty else "kline_cache_miss",
                    code=code,
                    cached_rows=0 if cached is None else len(cached),
                )
            ranges = _missing_ranges_for_cache(cached, pre_start, end_date)

            dfs = []
            for i, (segment_start, segment_end) in enumerate(ranges):
                segment_started_at = time.monotonic()
                self._log_progress(
                    "kline_segment_start",
                    code=code,
                    segment=f"{i + 1}/{len(ranges)}",
                    start=segment_start,
                    end=segment_end,
                )
                df = asyncio.run(adapter.get_kline(
                    code, period="daily",
                    count=0,
                    start_date=segment_start, end_date=segment_end,
                    adjustflag=self.cfg.adjustflag,
                ))
                self._log_progress(
                    "kline_segment_done",
                    code=code,
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
                    adjustflag=self.cfg.adjustflag,
                )
                if self._market_conn is not None:
                    self._market_conn.commit()
                self._log_progress("kline_cache_saved", code=code, rows=saved)
            self._log_progress(
                "kline_done",
                code=code,
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

        # 共同交易日（仅在回测区间内）
        all_dates = None
        for df in self._bars.values():
            dates = set(df["日期"].tolist())
            all_dates = dates if all_dates is None else all_dates & dates
        self._sorted_dates = sorted(d for d in (all_dates or []) if start_date <= d <= end_date)

        if not self._sorted_dates:
            return {"error": f"无共同交易日（区间 {start_date}~{end_date}）"}

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
    ) -> pd.DataFrame | None:
        if self._market_store is None:
            return None
        cached = self._market_store.get_price_bars(
            code,
            start=pre_start,
            end=end_date,
            adjustflag=self.cfg.adjustflag,
        )
        if cached.empty or (require_full and not _cache_covers_range(cached, pre_start, end_date)):
            return None
        cached = cached.copy().sort_values("日期").reset_index(drop=True)
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
        )

        index_config = {
            "clear_days_ma60": 15,
            "market_multipliers": self.cfg.market_multipliers,
        }

        for i, d in enumerate(self._sorted_dates):
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
            else:
                self._proxy_replay_dates.append(d)

            # ── 2. 持仓权益 ──────────────────────────────────────────
            portfolio_value = self._cash + sum(
                float(self._bars[code].set_index("日期").loc[d, "收盘"]) * pos.shares
                for code, pos in self._positions.items()
                if code in self._bars and d in self._bars[code]["日期"].values
            )

            # ── 3. 风控检查（止损/止盈/到期）─────────────────────────
            self._risk_check(d, i)

            # ── 4. 评分 + 决策 ───────────────────────────────────────
            current_exposure = (portfolio_value - self._cash) / portfolio_value if portfolio_value > 0 else 0.0
            if mirror_replay is not None:
                intents = mirror_replay["intents"]
            else:
                intents = []
                for code in self._bars:
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

                is_market_clear = market.multiplier == 0.0
                is_individual_clear = intent.action.value == "CLEAR"

                if not (is_market_clear or is_individual_clear):
                    continue

                pos = self._positions[score.code]
                df = self._bars[score.code]
                row = df[df["日期"] == d]
                if row.empty:
                    continue
                price = float(row["收盘"].iloc[0])

                if is_market_clear and not pos.market_reduced:
                    # 大盘 CLEAR/RED → 减仓 50%（每个持仓只减一次）
                    sell_shares = pos.shares // 2
                    pos.market_reduced = True
                    if sell_shares <= 0:
                        # 不足2手则全部清仓
                        sell_shares = pos.shares
                    reason = f"大盘{market.signal.value}减仓"
                    if sell_shares >= pos.shares:
                        self._positions.pop(score.code)
                    else:
                        pos.shares -= sell_shares
                else:
                    # 个股分数低 → 不强制卖，等止损/时间止损自然退出
                    continue

                pnl = (price - pos.entry_price) * sell_shares
                self._cash += price * sell_shares
                self._trades.append({
                    "date": d, "code": score.code, "name": score.name,
                    "side": "sell", "price": price, "shares": sell_shares,
                    "entry_price": pos.entry_price,
                    "pnl": round(pnl, 2),
                    "return_pct": round((price - pos.entry_price) / pos.entry_price * 100, 2),
                    "reason": reason,
                    "score": round(score.total, 1),
                })

            # ── 6. 执行 BUY 信号 ────────────────────────────────────
            if len(self._positions) < int(self.cfg.holding_max or 5) and self._cash > self.cfg.initial_cash * 0.05:
                buy_candidates = []
                for score, intent in intents:
                    route = getattr(score, "primary_strategy_route", None)
                    executable = self._intent_executable_for_backtest(
                        intent,
                        score.code,
                        route,
                        score_total=score.total,
                    )
                    if not executable:
                        self._record_execution_funnel_skip("not_executable", score, intent, market)
                        continue
                    if score.code in self._positions:
                        self._record_execution_funnel_skip("already_held", score, intent, market)
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

                daily_max_buys = int(self.cfg.daily_max_buys or 2)
                for index, (score, intent) in enumerate(buy_candidates):
                    if index >= daily_max_buys:
                        self._record_execution_funnel_skip("daily_limit", score, intent, market)
                        continue
                    if score.code in self._positions:
                        self._record_execution_funnel_skip("already_held", score, intent, market)
                        continue
                    if self._weekly_buy_count >= self._weekly_max_for_market(market):
                        self._record_execution_funnel_skip("weekly_limit", score, intent, market)
                        continue
                    df = self._bars[score.code]
                    row = df[df["日期"] == d]
                    if row.empty:
                        self._record_execution_funnel_skip("missing_price_row", score, intent, market)
                        continue
                    price = float(row["收盘"].iloc[0])
                    position_pct = self._execution_position_pct(
                        intent,
                        market,
                        route=getattr(score, "primary_strategy_route", None),
                    )
                    if position_pct <= 0:
                        self._record_execution_funnel_skip("zero_position_pct", score, intent, market)
                        continue
                    allocate = self._allocation_budget_for_position(d, position_pct)
                    shares = int(allocate / price / 100) * 100
                    if shares <= 0:
                        self._record_execution_funnel_skip("shares_zero", score, intent, market)
                        continue

                    self._cash -= price * shares
                    self._positions[score.code] = Position(
                        code=score.code,
                        shares=shares,
                        entry_price=price,
                        entry_date=d,
                        high_water=price,
                    )
                    self._weekly_buy_count += 1
                    self._trades.append({
                        "date": d, "code": score.code, "name": score.name,
                        "side": "buy", "price": price, "shares": shares,
                        "score": round(score.total, 1),
                        "source_action": intent.action.value,
                        "source_route": getattr(score, "primary_strategy_route", None) or "unknown",
                        "position_pct": round(position_pct, 4),
                        "pnl": 0, "return_pct": 0,
                    })
                    self._record_execution_funnel_buy(score, intent, market)
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
                    ):
                        self._record_execution_funnel_skip(block_reason, score, intent, market)

            # ── 7. 记录权益曲线 ─────────────────────────────────────
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
            route = str(getattr(score, "primary_strategy_route", "") or "unknown")
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
            _counter_inc(bucket["actions"], action)

    def _record_execution_funnel_executable(
        self,
        score: Any,
        intent: Any,
        market: "MarketState",
    ) -> None:
        self._execution_funnel["executable_candidates"] += 1
        bucket = self._execution_funnel_bucket(
            _market_signal_value(market),
            str(getattr(score, "primary_strategy_route", "") or "unknown"),
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
            str(getattr(score, "primary_strategy_route", "") or "unknown"),
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
            str(getattr(score, "primary_strategy_route", "") or "unknown"),
        )
        bucket["executed_buys"] += 1
        action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
        _counter_inc(bucket["executed_actions"], action)

    def _execution_funnel_bucket(self, signal: str, route: str) -> dict[str, Any]:
        key = f"{signal}:{route}"
        bucket = self._execution_funnel["by_market_route"].setdefault(
            key,
            {
                "signals": 0,
                "executable_candidates": 0,
                "executed_buys": 0,
                "actions": {},
                "executable_actions": {},
                "executed_actions": {},
                "skipped_actions": {},
                "skip_reasons": {},
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
    ) -> bool:
        action = str(getattr(getattr(intent, "action", None), "value", "") or "")
        if action == "BUY":
            return True
        signal = str(getattr(getattr(intent, "market_signal", None), "value", "") or "")
        route_name = str(route or "unknown")

        if action == "TRIAL_BUY":
            policy = self._route_execution_policy(signal, route_name)
            if policy:
                if not self._route_policy_allows_action(policy, action):
                    return False
                total = float(score_total if score_total is not None else getattr(intent, "score", 0.0) or 0.0)
                return (
                    total >= float(policy.get("score_min", self.cfg.execute_watch_trial_score_min or 0.0) or 0.0)
                    and code not in self._positions
                )
            allowed_routes = {str(item) for item in self.cfg.execute_trial_buy_routes if str(item)}
            if allowed_routes and route_name not in allowed_routes:
                return False
            return (
                signal in set(self.cfg.execute_trial_buy_market_signals or ())
                and code not in self._positions
            )

        if action != "WATCH":
            return False

        policy = self._route_execution_policy(signal, route_name)
        if policy:
            if not self._route_policy_allows_action(policy, action):
                return False
            total = float(score_total if score_total is not None else getattr(intent, "score", 0.0) or 0.0)
            return (
                total >= float(policy.get("score_min", self.cfg.execute_watch_trial_score_min or 0.0) or 0.0)
                and code not in self._positions
            )

        allowed_pairs = {str(item).strip() for item in self.cfg.execute_watch_trial_pairs if str(item).strip()}
        if allowed_pairs:
            if f"{signal}:{route_name}" not in allowed_pairs:
                return False
        else:
            allowed_watch_signals = set(self.cfg.execute_watch_trial_market_signals or ())
            if signal not in allowed_watch_signals:
                return False

            allowed_watch_routes = {str(item) for item in self.cfg.execute_watch_trial_routes if str(item)}
            if allowed_watch_routes and route_name not in allowed_watch_routes:
                return False

        total = float(score_total if score_total is not None else getattr(intent, "score", 0.0) or 0.0)
        return (
            total >= float(self.cfg.execute_watch_trial_score_min or 0.0)
            and code not in self._positions
        )

    def _execution_position_pct(self, intent: Any, market: "MarketState", route: str | None = None) -> float:
        action = str(getattr(getattr(intent, "action", None), "value", "") or "")
        signal = str(getattr(getattr(market, "signal", None), "value", "") or "")
        policy = self._route_execution_policy(signal, route)
        if (
            action in {"WATCH", "TRIAL_BUY"}
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
            return round(self.cfg.single_max_pct * float(getattr(market, "multiplier", 0.0) or 0.0), 4)
        return 0.0

    def _route_execution_policy(self, signal: str, route: str | None) -> dict:
        route_name = str(route or "unknown")
        policy_map = self.cfg.route_execution_policy or {}
        for key in (f"{signal}:{route_name}", f"*:{route_name}", route_name):
            policy = policy_map.get(key)
            if isinstance(policy, dict):
                return policy
        return {}

    def _route_policy_allows_action(self, policy: dict, action: str) -> bool:
        actions = policy.get("actions")
        if actions is None:
            return action == "WATCH"
        allowed = {str(item) for item in actions or () if str(item)}
        return action in allowed

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
        policy = self._route_execution_policy(signal, route)
        route_priority = float(policy.get("priority", 0.0) or 0.0) if self._route_policy_allows_action(policy, action) else 0.0
        return (action_priority + route_priority, float(score_total or 0.0), float(getattr(intent, "confidence", 0.0) or 0.0))

    def _weekly_max_for_market(self, market: "MarketState") -> int:
        signal = str(getattr(getattr(market, "signal", None), "value", "") or "")
        override = (self.cfg.weekly_max_by_market or {}).get(signal)
        if override is None:
            return int(self.cfg.weekly_max)
        return int(override)

    def _positions_market_value(self, trade_date: str) -> float:
        value = 0.0
        for code, pos in self._positions.items():
            df = self._bars.get(code)
            if df is None:
                continue
            close = self._close_on(df, trade_date)
            if close is None:
                continue
            value += close * pos.shares
        return value

    def _allocation_budget_for_position(self, trade_date: str, position_pct: float) -> float:
        position_value = self._positions_market_value(trade_date)
        portfolio_value = self._cash + position_value
        if portfolio_value <= 0:
            return 0.0
        single_budget = portfolio_value * max(0.0, float(position_pct or 0.0))
        total_budget = portfolio_value * float(self.cfg.total_max_pct or 0.0)
        remaining_total_budget = max(0.0, total_budget - position_value)
        return round(max(0.0, min(self._cash, single_budget, remaining_total_budget)), 2)

    def _mirror_replay_for_date(self, trade_date: str, fallback_market: "MarketState") -> dict | None:
        """优先读取真实历史信号镜像；没有镜像时让调用方回退到 proxy replay。"""
        if self._history_conn is None:
            return None

        from astock_trading.platform.history_mirror import load_signal_history_bundle
        from astock_trading.strategy.models import (
            Action,
            DataQuality,
            DecisionIntent,
            ScoreResult,
        )

        bundle = load_signal_history_bundle(self._history_conn, snapshot_date=trade_date)
        if not bundle:
            return None

        sections = bundle.get("sections") or {}
        market = _market_state_from_history_bundle(sections.get("market") or {}, fallback_market)
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
            self._signal_records.append({
                "code": getattr(score, "code", ""),
                "name": getattr(score, "name", ""),
                "signal_date": trade_date,
                "action": action or "WATCH",
                "score": round(float(getattr(score, "total", 0.0) or 0.0), 2),
                "entry_signal": entry_signal,
                "primary_strategy_route": route or "unknown",
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
        df = self._bars.get(code)
        if df is None or trade_date not in self._sorted_dates:
            return {}
        current_close = self._close_on(df, trade_date)
        if current_close is None or current_close <= 0:
            return {}
        start_idx = self._sorted_dates.index(trade_date)
        returns: dict[str, float] = {}
        for horizon in horizons:
            target_idx = start_idx + horizon
            if target_idx >= len(self._sorted_dates):
                continue
            target_close = self._close_on(df, self._sorted_dates[target_idx])
            if target_close is None:
                continue
            returns[f"{horizon}d"] = round((target_close - current_close) / current_close, 6)
        return returns

    @staticmethod
    def _close_on(df: pd.DataFrame, trade_date: str) -> float | None:
        row = df[df["日期"] == trade_date]
        if row.empty:
            return None
        return float(row["收盘"].iloc[0])

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

    def _risk_check(self, d: str, day_idx: int):
        """风控检查：止损/追踪止损/时间止损。"""
        to_close = []
        for code, pos in list(self._positions.items()):
            df = self._bars.get(code)
            if df is None:
                continue
            row = df[df["日期"] == d]
            if row.empty:
                continue

            price = float(row["收盘"].iloc[0])
            ret = (price - pos.entry_price) / pos.entry_price

            # 时间止损
            entry_idx = self._sorted_dates.index(pos.entry_date) if pos.entry_date in self._sorted_dates else 0
            days_held = day_idx - entry_idx

            # 追踪止损
            pos.high_water = max(pos.high_water, price)
            trail_drawdown = (price - pos.high_water) / pos.high_water if pos.high_water > 0 else 0.0

            stop_loss_triggered = ret <= -self.cfg.stop_loss
            trail_stop_triggered = trail_drawdown <= -self.cfg.trailing_stop
            time_stop_triggered = days_held >= self.cfg.time_stop_days

            if stop_loss_triggered or trail_stop_triggered or time_stop_triggered:
                if stop_loss_triggered:
                    reason = "止损"
                elif trail_stop_triggered:
                    reason = "追踪止损"
                else:
                    reason = "到期"
                to_close.append((code, price, ret, pos, reason))

        for code, price, ret, pos, reason in to_close:
            self._positions.pop(code)
            pnl = (price - pos.entry_price) * pos.shares
            self._cash += price * pos.shares
            self._trades.append({
                "date": d, "code": code,
                "side": "sell", "price": price, "shares": pos.shares,
                "entry_price": pos.entry_price,
                "pnl": round(pnl, 2),
                "return_pct": round(ret * 100, 2),
                "reason": reason,
                "score": 0,
            })

    def _load_financials(self, codes: list[str], start_date: str, end_date: str):
        """从 baostock 拉取区间内可用的季度财务快照并缓存。

        字段来源（均返回小数，如 0.128902 = 12.89%）：
        - roeAvg        → query_profit_data
        - YOYNI         → query_growth_data
        - CFOToOR       → query_cash_flow_data
        """
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
            df = self._bars.get(code)
            if df is not None and last_date in df["日期"].values:
                price = float(df[df["日期"] == last_date]["收盘"].iloc[0])
                final_value += price * pos.shares

        total_return = (final_value - self.cfg.initial_cash) / self.cfg.initial_cash * 100

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

        # 夏普比率（简化：日收益 / 日波动）
        if len(equity_series) > 2:
            rets = pd.Series(equity_series).pct_change().dropna()
            ann_ret = rets.mean() * 252 if len(rets) > 0 else 0
            ann_vol = rets.std() * math.sqrt(252) if len(rets) > 1 else 1
            sharpe = (ann_ret / ann_vol) if ann_vol > 0 else 0
        else:
            sharpe = 0.0

        ann_return = total_return / max(len(self._sorted_dates) / 252, 0.01)
        calmar = ann_return / max_dd if max_dd > 0 else 0.0
        unknown_route_signals = [
            item for item in self._signal_records
            if item.get("primary_strategy_route") == "unknown"
        ]
        if self.cfg.signal_record_limit is None:
            signal_rows = list(self._signal_records)
        elif self.cfg.signal_record_limit <= 0:
            signal_rows = []
        else:
            signal_rows = self._signal_records[-int(self.cfg.signal_record_limit):]

        return {
            "preset": self.cfg.preset_name,
            "initial_cash": self.cfg.initial_cash,
            "final_value": round(final_value, 2),
            "total_return_pct": round(total_return, 2),
            "annual_return_pct": round(ann_return, 2),
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
            "data_coverage": {
                "requested_codes": len(self._requested_codes),
                "loaded_codes": len(self._loaded_codes),
                "loaded_code_list": self._loaded_codes,
            },
            "execution_funnel": self._execution_funnel,
            "signal_alpha": (
                signal_alpha_summary(self._signal_records)
                if self.cfg.include_signal_alpha
                else {"skipped": True, "sample_size": len(self._signal_records)}
            ),
            "signal_validation": {
                "sample_size": len(self._signal_records),
                "signals": signal_rows,
                "unknown_route_count": len(unknown_route_signals),
                "unknown_route_samples": unknown_route_signals[-20:],
            },
            "equity_curve": self._portfolio_value_series,
            "trades": self._trades[-50:],
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
        weights=weights,
        veto_rules=veto_rules,
        decision_gates=decision_gates,
        market_regime_overlays=market_regime_overlays,
        score_adjustments=score_adjustments,
    )


def run_backtest(
    codes: str,
    start: str,
    end: str,
    preset: str = "保守验证C",
    initial_cash: float = 100000.0,
    adjustflag: str = "2",
    use_history_mirror: bool = True,
    history_db_path: Optional[Path] = None,
    red_multiplier: float | None = None,
    execute_red_trial_buy: bool = False,
    execute_trial_buy_routes: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_markets: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_routes: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_pairs: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_score_min: float = 6.0,
    trailing_stop: float | None = None,
    signal_record_limit: int | None = 50,
    include_signal_alpha: bool = True,
    load_financials: bool = True,
    progress_log: bool = False,
    use_stored_data: bool = False,
    hydrate_data: bool = False,
    use_market_bars: bool = False,
    hydrate_market_bars: bool = False,
    score_dimension_mode: str = "full",
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
    if trailing_stop is not None:
        cfg.trailing_stop = float(trailing_stop)
    if red_multiplier is not None:
        cfg.market_multipliers = {**cfg.market_multipliers, "RED": red_multiplier}
    if execute_red_trial_buy:
        cfg.execute_trial_buy_market_signals = tuple(
            dict.fromkeys([*cfg.execute_trial_buy_market_signals, "RED"])
        )
    cfg.execute_trial_buy_routes = tuple(str(item) for item in (execute_trial_buy_routes or ()) if str(item))
    cfg.execute_watch_trial_market_signals = tuple(
        str(item) for item in (execute_watch_trial_markets or ()) if str(item)
    )
    cfg.execute_watch_trial_routes = tuple(
        str(item) for item in (execute_watch_trial_routes or ()) if str(item)
    )
    cfg.execute_watch_trial_pairs = tuple(
        str(item) for item in (execute_watch_trial_pairs or ()) if str(item)
    )
    cfg.execute_watch_trial_score_min = float(execute_watch_trial_score_min or 0.0)
    cfg.signal_record_limit = signal_record_limit
    cfg.include_signal_alpha = bool(include_signal_alpha)
    cfg.score_dimension_mode = score_dimension_mode
    cfg.load_financials = load_financials
    cfg.progress_log = progress_log
    cfg.use_market_bars = use_market_bars or use_stored_data or hydrate_data
    cfg.hydrate_market_bars = hydrate_market_bars or hydrate_data
    cfg.use_financial_cache = use_stored_data or hydrate_data
    cfg.hydrate_financial_cache = hydrate_data

    history_conn = _open_history_connection(use_history_mirror, history_db_path)
    market_conn = _open_market_data_connection(
        use_market_bars=cfg.use_market_bars,
        hydrate_market_bars=cfg.hydrate_market_bars,
        use_financial_cache=cfg.use_financial_cache,
        hydrate_financial_cache=cfg.hydrate_financial_cache,
    )
    # 初始化引擎
    engine = BacktestEngine(cfg, history_conn=history_conn, market_conn=market_conn)
    try:
        # 向前多拉 90 天用于 MA 计算
        from datetime import date as date_type, timedelta as td
        pre_start = (date_type.fromisoformat(start) - td(days=90)).isoformat()

        load_result = engine.load_data(code_list, start, end, pre_start)
        if "error" in load_result:
            return load_result

        return engine.run()
    finally:
        if history_conn is not None:
            history_conn.close()
        if market_conn is not None:
            market_conn.close()


def _open_history_connection(use_history_mirror: bool, history_db_path: Optional[Path]):
    if not use_history_mirror:
        return None
    try:
        from astock_trading.platform.db import connect

        return connect(history_db_path) if history_db_path else connect()
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
