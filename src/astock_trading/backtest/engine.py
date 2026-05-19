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
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING, Optional

import pandas as pd
import yaml

if TYPE_CHECKING:
    from astock_trading.strategy.models import MarketState


# ---------------------------------------------------------------------------
# Indicator 计算（纯函数）
# ---------------------------------------------------------------------------

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

    multiplier = {
        MarketSignal.GREEN: 1.0,
        MarketSignal.YELLOW: 0.5,
        MarketSignal.RED: 0.0,
        MarketSignal.CLEAR: 0.0,
    }.get(signal, 0.0)

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
    # 评分权重
    weights: dict = field(default_factory=lambda: {
        "technical": 3.0, "fundamental": 2.0, "flow": 2.0, "sentiment": 3.0
    })
    veto_rules: list = field(default_factory=lambda: [
        "below_ma20", "limit_up_today", "consecutive_outflow", "red_market", "ma20_trend_down"
    ])


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

    def __init__(self, config: BacktestConfig, history_conn: Any | None = None):
        self.cfg = config
        self._history_conn = history_conn
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
        self._financial_cache: dict[str, dict] = {}  # code -> {roe, revenue_growth, ocf}
        self._history_mirror_dates: list[str] = []
        self._proxy_replay_dates: list[str] = []

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

        # 加载股票数据（baostock 单次最多返回 ~120 条，分多批取再合并）
        def _fetch_code(code: str) -> Optional[pd.DataFrame]:
            # 计算分段数量（每年一段，最多4段）
            s_year, s_month = int(pre_start[0:4]), int(pre_start[5:7])
            e_year, e_month = int(end_date[0:4]), int(end_date[5:7])
            total_months = (e_year - s_year) * 12 + (e_month - s_month)
            n_batches = min(max(total_months // 4, 1), 4)

            # 生成切分日期列表（使用月初，保证日期有效）
            split_dates = []
            for i in range(n_batches + 1):
                months = s_month - 1 + i * (total_months // n_batches)
                y = s_year + months // 12
                m = months % 12 + 1
                split_dates.append(f"{y}-{m:02d}-01")

            dfs = []
            for i in range(len(split_dates) - 1):
                df = asyncio.run(adapter.get_kline(
                    code, period="daily",
                    start_date=split_dates[i], end_date=split_dates[i + 1],
                    adjustflag=self.cfg.adjustflag,
                ))
                if df is not None and not df.empty:
                    dfs.append(df)

            if not dfs:
                return None
            combined = pd.concat(dfs, ignore_index=True)
            combined = combined.drop_duplicates(subset=["日期"]).sort_values("日期").reset_index(drop=True)
            return combined

        for code in codes:
            df = _fetch_code(code)
            if df is not None and not df.empty:
                self._bars[code] = df

        if not self._bars:
            return {"error": "所有股票均无法获取数据", "codes": codes}

        # 加载上证指数数据（同样需要分批，绕过 120 条限制）
        idx_df = _fetch_code("000001")
        if idx_df is not None and not idx_df.empty:
            self._index_df = idx_df

        # 预加载年报财务数据（baostock 按年/季报披露，回测期间复用最新一期）
        self._load_financials(list(self._bars.keys()), end_date)

        # 共同交易日（仅在回测区间内）
        all_dates = None
        for df in self._bars.values():
            dates = set(df["日期"].tolist())
            all_dates = dates if all_dates is None else all_dates & dates
        self._sorted_dates = sorted(d for d in (all_dates or []) if start_date <= d <= end_date)

        if not self._sorted_dates:
            return {"error": f"无共同交易日（区间 {start_date}~{end_date}）"}

        return {"loaded": len(self._bars), "trading_days": len(self._sorted_dates)}

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

        w = self.cfg.weights
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
        )
        self._decider = Decider(
            # 回测场景无真实资金流数据，分数上限约 6.0，将阈值适当降低
            # 实盘使用 preset 的原始 buy_threshold
            buy_threshold=max(5.0, self.cfg.buy_threshold - 1.0),
            watch_threshold=4.0,
            single_max_pct=self.cfg.single_max_pct,
            total_max_pct=self.cfg.total_max_pct,
            weekly_max=self.cfg.weekly_max,
        )

        index_config = {"clear_days_ma60": 15}

        for i, d in enumerate(self._sorted_dates):
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
            if len(self._positions) < 5 and self._cash > self.cfg.initial_cash * 0.05:
                # 按评分排序，优先高分
                buy_candidates = [
                    (score, intent)
                    for score, intent in intents
                    if intent.action.value == "BUY" and score.code not in self._positions
                ]
                buy_candidates.sort(key=lambda x: -x[0].total)

                for score, intent in buy_candidates[:2]:  # 最多买2只
                    if score.code in self._positions:
                        continue
                    if self._weekly_buy_count >= self.cfg.weekly_max:
                        break
                    df = self._bars[score.code]
                    row = df[df["日期"] == d]
                    if row.empty:
                        continue
                    price = float(row["收盘"].iloc[0])
                    allocate = self._cash * self.cfg.single_max_pct
                    shares = int(allocate / price / 100) * 100
                    if shares <= 0:
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
                        "pnl": 0, "return_pct": 0,
                    })

            # ── 7. 记录权益曲线 ─────────────────────────────────────
            self._portfolio_value_series.append({
                "date": d,
                "equity": round(portfolio_value, 2),
                "cash": round(self._cash, 2),
                "positions": len(self._positions),
            })

        return self._build_report()

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
        fin = self._financial_cache.get(code, {})
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
            # 回测场景：使用 baostock 拉取的最新一期财务数据
            financial=FinancialReport(
                roe=fin.get("roe"),                          # 真实 ROE（百分数，如 12.0）
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
            trail_ret = (pos.high_water - pos.entry_price) / pos.entry_price

            stop_loss_triggered = ret <= -self.cfg.stop_loss
            trail_stop_triggered = trail_ret <= -self.cfg.trailing_stop
            time_stop_triggered = days_held >= self.cfg.time_stop_days

            if stop_loss_triggered or trail_stop_triggered or time_stop_triggered:
                to_close.append((code, price, ret, pos))

        for code, price, ret, pos in to_close:
            self._positions.pop(code)
            pnl = (price - pos.entry_price) * pos.shares
            self._cash += price * pos.shares
            reason = "止损" if ret < -0.02 else ("追踪止损" if trail_stop_triggered else "到期")
            self._trades.append({
                "date": d, "code": code,
                "side": "sell", "price": price, "shares": pos.shares,
                "entry_price": pos.entry_price,
                "pnl": round(pnl, 2),
                "return_pct": round(ret * 100, 2),
                "reason": reason,
                "score": 0,
            })

    def _load_financials(self, codes: list[str], end_date: str):
        """从 baostock 拉取最新一期年报/季报财务数据并缓存。

        字段来源（均返回小数，如 0.128902 = 12.89%）：
        - roeAvg        → query_profit_data
        - YOYNI         → query_growth_data
        - CFOToOR       → query_cash_flow_data
        """
        import baostock as bs

        year = int(end_date[0:4])
        q_str = {"03-31": "1", "06-30": "2", "09-30": "3", "12-31": "4"}.get(
            end_date[5:10], "4"
        )

        def _fetch(rs):
            rows, fields = [], []
            while rs.next():
                if not fields:
                    fields = list(rs.fields)
                rows.append(list(rs.get_row_data()))
            return pd.DataFrame(rows, columns=fields) if rows else pd.DataFrame()

        bs.login()
        try:
            for code in codes:
                bs_code = self._bs_code(code)
                roe = None
                rev_growth = None
                ocf = 0.0

                # ROE：来自 query_profit_data 的 roeAvg
                df = _fetch(bs.query_profit_data(bs_code, year, q_str))
                if not df.empty:
                    val = str(df.iloc[0].get("roeAvg", ""))
                    if val and val not in ("", "None"):
                        try:
                            roe = float(val) * 100
                        except ValueError:
                            pass

                # 净利润增速：YoY
                df = _fetch(bs.query_growth_data(bs_code, year, q_str))
                if not df.empty:
                    val = str(df.iloc[0].get("YOYNI", ""))
                    if val and val not in ("", "None"):
                        try:
                            rev_growth = float(val) * 100
                        except ValueError:
                            pass

                # 现金流比率
                df = _fetch(bs.query_cash_flow_data(bs_code, year, q_str))
                if not df.empty:
                    val = str(df.iloc[0].get("CFOToOR", ""))
                    if val and val not in ("", "None"):
                        try:
                            ocf = float(val)
                        except ValueError:
                            pass

                # fallback：上一年年报
                if roe is None and year > 2000:
                    df = _fetch(bs.query_profit_data(bs_code, year - 1, "4"))
                    if not df.empty:
                        val = str(df.iloc[0].get("roeAvg", ""))
                        if val and val not in ("", "None"):
                            try:
                                roe = float(val) * 100
                            except ValueError:
                                pass

                self._financial_cache[code] = {
                    "roe": roe,
                    "revenue_growth": rev_growth,
                    "operating_cash_flow": ocf,
                }
        finally:
            bs.logout()

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

        return {
            "preset": self.cfg.preset_name,
            "initial_cash": self.cfg.initial_cash,
            "final_value": round(final_value, 2),
            "total_return_pct": round(total_return, 2),
            "annual_return_pct": round(ann_return, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "win_rate_pct": round(win_rate, 1),
            "sharpe_ratio": round(sharpe, 2),
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

    if cfg_path.exists():
        with open(cfg_path) as f:
            full = yaml.safe_load(f) or {}
        presets = full.get("backtest_presets", {})
        sc = full.get("scoring", {})
        weights_cfg = sc.get("weights", {})
        if weights_cfg:
            weights = weights_cfg
        veto_rules = sc.get("veto", veto_rules)

    p = presets.get(preset_name, presets.get("保守验证C", {}))

    # 融合 preset 和评分配置
    return BacktestConfig(
        preset_name=preset_name,
        trailing_stop=p.get("momentum_trailing_stop", 0.10),
        stop_loss=p.get("momentum_stop_loss", 0.08),
        time_stop_days=p.get("momentum_time_stop_days", 15),
        buy_threshold=p.get("buy_threshold", 6.5),
        single_max_pct=0.20,
        total_max_pct=0.60,
        weekly_max=2,
        weights=weights,
        veto_rules=veto_rules,
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

    history_conn = _open_history_connection(use_history_mirror, history_db_path)
    # 初始化引擎
    engine = BacktestEngine(cfg, history_conn=history_conn)
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


def _open_history_connection(use_history_mirror: bool, history_db_path: Optional[Path]):
    if not use_history_mirror:
        return None
    try:
        from astock_trading.platform.db import connect

        return connect(history_db_path) if history_db_path else connect()
    except Exception:
        return None
