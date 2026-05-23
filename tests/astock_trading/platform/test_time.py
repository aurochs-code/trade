from __future__ import annotations

from datetime import date
import logging

from astock_trading.platform import time as time_helpers


def test_is_trading_day_akshare_calendar_failure_uses_quiet_fallback(monkeypatch, caplog):
    def broken_calendar():
        raise RuntimeError("py_mini_racer unavailable")

    monkeypatch.setattr(time_helpers.ak, "tool_trade_date_hist_sina", broken_calendar)
    monkeypatch.setattr(time_helpers, "_fallback_chinese_calendar", lambda target: True)
    monkeypatch.setattr(time_helpers, "_CACHE_LOADED", False)
    monkeypatch.setattr(time_helpers, "_TRADING_DATE_CACHE", None)

    with caplog.at_level(logging.WARNING):
        result = time_helpers.is_trading_day(date(2026, 5, 22))

    assert result is True
    assert not any("无法从 AkShare 加载日历" in record.message for record in caplog.records)
