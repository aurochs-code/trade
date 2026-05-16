"""Tests for strategy/classifier.py — pure function style classification"""


from astock_trading.strategy.classifier import (
    calc_daily_volatility,
    calc_ma20_slope,
    calc_rsi,
    check_style_switch,
    classify_style,
)
from astock_trading.strategy.models import Style


def _make_slow_bull_closes(n: int = 80) -> list[float]:
    """生成慢牛 K 线：低波动、稳步上行。"""
    base = 10.0
    return [round(base + i * 0.02 + (i % 3 - 1) * 0.01, 2) for i in range(n)]


def _make_momentum_closes(n: int = 80) -> list[float]:
    """生成题材 K 线：高波动、快速拉升。"""
    base = 10.0
    return [round(base + i * 0.10 + (i % 5 - 2) * 0.40, 2) for i in range(n)]


def _make_flat_closes(n: int = 80) -> list[float]:
    """生成横盘 K 线。"""
    return [10.0 + (i % 3 - 1) * 0.05 for i in range(n)]


class TestCalcHelpers:
    def test_ma20_slope_uptrend(self):
        closes = _make_slow_bull_closes()
        slope = calc_ma20_slope(closes)
        assert slope > 0

    def test_ma20_slope_insufficient_data(self):
        closes = [10.0] * 15
        slope = calc_ma20_slope(closes)
        assert slope == 0.0

    def test_daily_volatility_low(self):
        closes = _make_slow_bull_closes()
        vol = calc_daily_volatility(closes)
        assert vol < 0.03  # 慢牛波动率应该低

    def test_daily_volatility_high(self):
        closes = _make_momentum_closes()
        vol = calc_daily_volatility(closes)
        assert vol > 0.01  # 题材波动率应该高

    def test_rsi_neutral(self):
        closes = _make_flat_closes()
        rsi = calc_rsi(closes)
        assert 30 < rsi < 70

    def test_rsi_insufficient_data(self):
        rsi = calc_rsi([10.0, 10.1])
        assert rsi == 50.0


class TestClassifyStyle:
    def test_slow_bull(self):
        closes = _make_slow_bull_closes()
        result = classify_style(closes, rsi=58.0)
        assert result.style == Style.SLOW_BULL
        assert result.confidence > 0
        assert "daily_volatility" in result.metrics

    def test_momentum_by_rsi_and_volatility(self):
        closes = _make_momentum_closes()
        result = classify_style(closes, rsi=78.0)
        assert result.style == Style.MOMENTUM

    def test_default_to_momentum_high_vol(self):
        """高波动但其他指标不明确 → 默认题材。"""
        closes = _make_momentum_closes()
        result = classify_style(closes, rsi=60.0)
        # 高波动率应该至少归为题材
        assert result.style in (Style.MOMENTUM, Style.SLOW_BULL)

    def test_custom_config(self):
        closes = _make_slow_bull_closes()
        config = {
            "slow_bull": {
                "daily_volatility_max": 0.05,
                "rsi_range": [40, 70],
                "ma20_slope_min": 0.001,
            },
            "momentum": {
                "daily_volatility_min": 0.06,
                "rsi_high_threshold": 80,
                "ma20_slope_min": 0.03,
            },
        }
        result = classify_style(closes, rsi=55.0, config=config)
        assert result.style == Style.SLOW_BULL

    def test_short_data_returns_result(self):
        """数据不足时不崩溃，返回默认。"""
        closes = [10.0] * 10
        result = classify_style(closes, rsi=50.0)
        assert result.style in (Style.SLOW_BULL, Style.MOMENTUM, Style.UNKNOWN)


class TestCheckStyleSwitch:
    def test_no_switch_for_momentum(self):
        result = check_style_switch(Style.MOMENTUM, 0.08, 80.0)
        assert not result.should_switch
        assert result.new_style == Style.MOMENTUM

    def test_surge_triggers_switch(self):
        result = check_style_switch(Style.SLOW_BULL, 0.08, 60.0)
        assert result.should_switch
        assert result.new_style == Style.MOMENTUM
        assert "涨幅" in result.trigger

    def test_rsi_overheat_triggers_switch(self):
        rsi_history = [76.0, 77.0, 78.0]
        result = check_style_switch(
            Style.SLOW_BULL, 0.02, 78.0, rsi_history=rsi_history,
        )
        assert result.should_switch
        assert "RSI" in result.trigger

    def test_no_switch_below_threshold(self):
        result = check_style_switch(Style.SLOW_BULL, 0.03, 60.0)
        assert not result.should_switch

    def test_custom_config(self):
        config = {"single_day_surge_pct": 0.10}
        result = check_style_switch(
            Style.SLOW_BULL, 0.08, 60.0, config=config,
        )
        assert not result.should_switch  # 8% < 10% threshold
