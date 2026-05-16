"""
market/indicators.py — 从 K 线 DataFrame 计算技术指标

akshare stock_zh_a_hist 返回列：
    日期, 股票代码, 开盘, 收盘, 最高, 最低, 成交量, 成交额,
    振幅, 涨跌幅, 涨跌额, 换手率
"""

from __future__ import annotations

from typing import Optional
import pandas as pd

from astock_trading.market.models import StockQuote, TechnicalIndicators


def compute_technical_indicators(kline: pd.DataFrame, quote: Optional[StockQuote] = None) -> TechnicalIndicators:
    """
    从日 K 线 DataFrame 计算技术指标。

    Args:
        kline: akshare 返回的日线 DataFrame，按日期升序排列
        quote: 当前行情（用于 above_ma20 等实时判断）
    """
    if kline is None or kline.empty:
        return TechnicalIndicators()

    df = kline.copy()

    # 标准化列名（akshare 中文列名 或 stock_zh_a_daily 英文列名）
    col_rename = {}
    for c in df.columns:
        c_str = str(c)
        if c_str in ("日期", "date"):
            col_rename[c] = "date"
        elif c_str in ("开盘", "open"):
            col_rename[c] = "open"
        elif c_str in ("收盘", "close"):
            col_rename[c] = "close"
        elif c_str in ("最高", "high"):
            col_rename[c] = "high"
        elif c_str in ("最低", "low"):
            col_rename[c] = "low"
        elif c_str in ("成交量", "volume"):
            col_rename[c] = "volume"
        elif c_str in ("成交额", "amount"):
            col_rename[c] = "amount"
        elif c_str in ("涨跌幅", "pct_change"):
            col_rename[c] = "涨跌幅"
    if col_rename:
        df = df.rename(columns=col_rename)

    # 确保数值列
    for col in ["open", "close", "high", "low", "volume", "amount", "涨跌幅"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    close = df["close"]
    volume = df["volume"]

    # 移动平均
    ma5 = _ma(close, 5)
    ma10 = _ma(close, 10)
    ma20 = _ma(close, 20)
    ma60 = _ma(close, 60)

    # 最新值
    c0 = float(close.iloc[-1]) if len(close) >= 1 else 0.0
    v0 = float(volume.iloc[-1]) if len(volume) >= 1 else 0.0

    # 量比：今日成交量 / 5日均量
    vol_ma5 = volume.rolling(5).mean()
    vol_ma5_val = float(vol_ma5.iloc[-1]) if len(vol_ma5) >= 1 and vol_ma5.iloc[-1] > 0 else 1.0
    volume_ratio = round(v0 / vol_ma5_val, 2) if vol_ma5_val > 0 else 1.0

    # RSI(14)
    rsi = _rsi(close, 14)

    # 金叉：MA5 上穿 MA10（前一日 MA5 <= MA10，今日 MA5 > MA10）
    # 用 rolling Series 直接取倒数第二个值，避免重复计算
    golden_cross = False
    if len(close) >= 11 and ma5 > ma10 > 0:
        ma5_series = close.rolling(5).mean()
        ma10_series = close.rolling(10).mean()
        prev_ma5 = float(ma5_series.iloc[-2]) if len(ma5_series) >= 2 else 0.0
        prev_ma10 = float(ma10_series.iloc[-2]) if len(ma10_series) >= 2 else 0.0
        golden_cross = bool(prev_ma5 <= prev_ma10 and ma5 > ma10)

    # MA20 斜率（需要用 Series 计算）
    ma20_slope = 0.0
    ma20_series = close.rolling(20).mean()
    if len(ma20_series) >= 10:
        ma20_dropped = ma20_series.dropna()
        if len(ma20_dropped) >= 5:
            ma20_slope = round((ma20_dropped.iloc[-1] - ma20_dropped.iloc[-5]) / ma20_dropped.iloc[-5], 4)

    # 5日动量
    momentum_5d = 0.0
    if len(close) >= 6:
        momentum_5d = round((float(close.iloc[-1]) - float(close.iloc[-6])) / float(close.iloc[-6]) * 100, 2)

    # 日内波动率
    daily_volatility = 0.0
    if "high" in df.columns and "low" in df.columns:
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        if len(high) >= 1 and len(low) >= 1:
            daily_volatility = round((float(high.iloc[-1]) - float(low.iloc[-1])) / float(close.iloc[-1]) if float(close.iloc[-1]) > 0 else 0.0, 4)

    # 偏离率：现价相对 MA20
    deviation_rate = 0.0
    if ma20 > 0:
        deviation_rate = round((c0 - ma20) / ma20 * 100, 2)

    # above_ma20（优先用实时行情）
    if quote is not None:
        above_ma20 = bool(quote.close > ma20 > 0)
        change_pct = float(quote.change_pct)
    else:
        above_ma20 = bool(c0 > ma20 > 0)
        if "涨跌幅" in df.columns:
            change_pct = float(df["涨跌幅"].iloc[-1])
        else:
            change_pct = 0.0

    return TechnicalIndicators(
        ma5=round(ma5, 2) if ma5 > 0 else 0.0,
        ma10=round(ma10, 2) if ma10 > 0 else 0.0,
        ma20=round(ma20, 2) if ma20 > 0 else 0.0,
        ma60=round(ma60, 2) if ma60 > 0 else 0.0,
        above_ma20=above_ma20,
        volume_ratio=volume_ratio,
        rsi=round(rsi, 1),
        golden_cross=golden_cross,
        ma20_slope=ma20_slope,
        momentum_5d=momentum_5d,
        daily_volatility=daily_volatility,
        deviation_rate=deviation_rate,
        change_pct=change_pct,
    )


def _ma(series: pd.Series, window: int) -> float:
    if len(series) < window:
        return 0.0
    return float(series.rolling(window).mean().iloc[-1])


def _rsi(series: pd.Series, window: int = 14) -> float:
    if len(series) < window + 1:
        return 50.0
    deltas = series.diff()
    gains = deltas.clip(lower=0)
    losses = (-deltas).clip(lower=0)
    avg_gain = gains.rolling(window).mean().iloc[-1]
    avg_loss = losses.rolling(window).mean().iloc[-1]
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 1)
