"""Tests for strategy/timer.py — pure function market timing"""


from astock_trading.strategy.models import MarketSignal
from astock_trading.strategy.timer import compute_market_signal


def test_green_signal():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"above_ma20": True, "below_ma60_days": 0},
        "创业板指": {"above_ma20": True, "below_ma60_days": 0},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.GREEN
    assert state.multiplier == 1.0


def test_red_signal():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 3},
        "深证成指": {"above_ma20": False, "below_ma60_days": 5},
        "创业板指": {"above_ma20": False, "below_ma60_days": 2},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.RED
    assert state.multiplier == 0.0


def test_missing_ma20_uses_intraday_change_without_false_red():
    """MA 数据缺失时，不应把 above_ma20 缺失直接当成跌破均线。"""
    import astock_trading.strategy.timer as _timer
    _timer._last_valid_state = None

    index_data = {
        "上证指数": {"change_pct": 0.47, "ma20": None, "below_ma60_days": 0},
        "深证成指": {"change_pct": 1.39, "ma20": None, "below_ma60_days": 0},
        "创业板指": {"change_pct": 1.91, "ma20": None, "below_ma60_days": 0},
        "失真指数": {"price": 0, "change_pct": 0, "ma20": None, "below_ma60_days": 0},
    }
    state = compute_market_signal(index_data)

    assert state.signal == MarketSignal.YELLOW
    assert state.multiplier == 0.5
    assert state.detail["fallback_intraday_count"] == 3
    assert state.detail["total"] == 3
    assert "MA 数据缺失" in state.detail["reason"]


def test_yellow_signal():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"above_ma20": False, "below_ma60_days": 3},
        "创业板指": {"above_ma20": False, "below_ma60_days": 2},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.YELLOW
    assert state.multiplier == 0.5


def test_clear_signal():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 20},
        "深证成指": {"above_ma20": False, "below_ma60_days": 18},
        "创业板指": {"above_ma20": False, "below_ma60_days": 25},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.CLEAR
    assert state.multiplier == 0.0


def test_no_data_first_time():
    """No data and no history → RED (not CLEAR, to avoid false liquidation)."""
    # Reset module-level cache
    import astock_trading.strategy.timer as _timer
    _timer._last_valid_state = None

    state = compute_market_signal({})
    assert state.signal == MarketSignal.RED
    assert state.multiplier == 0.0


def test_no_data_fallback_to_last_valid():
    """No data but has previous valid signal → fallback to last signal."""
    import astock_trading.strategy.timer as _timer
    _timer._last_valid_state = None

    # First: establish a valid GREEN signal
    green_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"above_ma20": True, "below_ma60_days": 0},
        "创业板指": {"above_ma20": True, "below_ma60_days": 0},
    }
    state = compute_market_signal(green_data)
    assert state.signal == MarketSignal.GREEN

    # Then: no data → should fallback to GREEN
    state = compute_market_signal({})
    assert state.signal == MarketSignal.GREEN
    assert "沿用上次信号" in state.detail.get("reason", "")


def test_no_data_fallback_preserves_red():
    """Fallback should also work for non-GREEN signals."""
    import astock_trading.strategy.timer as _timer
    _timer._last_valid_state = None

    red_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 3},
        "深证成指": {"above_ma20": False, "below_ma60_days": 5},
        "创业板指": {"above_ma20": False, "below_ma60_days": 2},
    }
    compute_market_signal(red_data)

    state = compute_market_signal({})
    assert state.signal == MarketSignal.RED


def test_partial_error():
    index_data = {
        "上证指数": {"above_ma20": True, "below_ma60_days": 0},
        "深证成指": {"error": "timeout"},
        "创业板指": {"above_ma20": True, "below_ma60_days": 0},
    }
    state = compute_market_signal(index_data)
    assert state.signal == MarketSignal.GREEN  # 2/2 valid are green


def test_custom_clear_days():
    index_data = {
        "上证指数": {"above_ma20": False, "below_ma60_days": 8},
        "深证成指": {"above_ma20": False, "below_ma60_days": 10},
        "创业板指": {"above_ma20": False, "below_ma60_days": 9},
    }
    state = compute_market_signal(index_data, config={"clear_days_ma60": 7})
    assert state.signal == MarketSignal.CLEAR
