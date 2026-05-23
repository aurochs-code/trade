from datetime import datetime
from zoneinfo import ZoneInfo

from astock_trading.pipeline.auto_trade import _trade_window_state


TZ = ZoneInfo("Asia/Shanghai")


def test_trade_window_state_allows_trade_inside_configured_window():
    cfg = {
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    }

    state = _trade_window_state(cfg, datetime(2026, 5, 15, 10, 0, tzinfo=TZ))

    assert state["buy_open"] is True
    assert state["sell_open"] is True


def test_trade_window_state_blocks_buy_before_buy_window():
    cfg = {
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    }

    state = _trade_window_state(cfg, datetime(2026, 5, 15, 9, 40, tzinfo=TZ))

    assert state["buy_open"] is False
    assert state["sell_open"] is True


def test_trade_window_state_keeps_end_minute_open_for_scheduled_runs():
    cfg = {
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    }

    state = _trade_window_state(cfg, datetime(2026, 5, 15, 14, 30, 45, tzinfo=TZ))

    assert state["buy_open"] is True
    assert state["sell_open"] is True


def test_trade_window_state_blocks_after_end_minute():
    cfg = {
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    }

    state = _trade_window_state(cfg, datetime(2026, 5, 15, 14, 31, tzinfo=TZ))

    assert state["buy_open"] is False
    assert state["sell_open"] is True


def test_trade_window_state_blocks_non_trading_day_even_inside_time_window():
    cfg = {
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    }

    state = _trade_window_state(cfg, datetime(2026, 5, 23, 10, 0, tzinfo=TZ))

    assert state["buy_open"] is False
    assert state["sell_open"] is False
    assert state["trading_day"] is False


def test_trade_window_state_defaults_open_without_config():
    state = _trade_window_state({}, datetime(2026, 5, 15, 8, 0, tzinfo=TZ))

    assert state["buy_open"] is True
    assert state["sell_open"] is True
