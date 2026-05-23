from __future__ import annotations

from datetime import datetime

from astock_trading.platform.manual_trade_state import manual_trade_states
from astock_trading.platform.time import MARKET_TZ


def test_manual_trade_states_marks_buy_signal_on_non_trading_day_stale():
    events = [
        {
            "event_id": "manual-weekend-buy",
            "event_type": "manual_trade.requested",
            "stream": "manual_trade:002384",
            "occurred_at": "2026-05-22T17:44:18+00:00",
            "payload": {
                "status": "pending",
                "side": "buy",
                "code": "002384",
                "name": "东山精密",
                "score": 7.0,
            },
        }
    ]

    states = manual_trade_states(
        events,
        policy={
            "pending_max_age_hours": 4,
            "buy_window": {"start": "09:45", "end": "14:30"},
        },
        now=datetime(2026, 5, 23, 2, 17, tzinfo=MARKET_TZ),
    )

    assert states[0]["stale"] is True
    assert states[0]["actionable"] is False
    assert states[0]["stale_reason"] == "non_trading_day"
    assert states[0]["stale_reason_label"] == "当前非交易日"
