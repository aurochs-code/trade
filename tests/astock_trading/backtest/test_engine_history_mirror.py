"""回测引擎优先读取历史信号镜像。"""

from __future__ import annotations

import pandas as pd

from astock_trading.backtest.engine import BacktestConfig, BacktestEngine
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.history_mirror import archive_signal_history
from astock_trading.strategy.models import MarketSignal, MarketState


def test_backtest_engine_uses_signal_history_before_proxy_replay(tmp_path):
    db_path = tmp_path / "history.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-01-05",
            history_group_id="hist_20260105_screener",
            run_id="screener_101500",
            phase="screener",
            market={"signal": "GREEN", "multiplier": 1.0, "detail": {"source": "test"}},
            candidates=[{"code": "600036", "name": "招商银行", "total_score": 7.2}],
            decisions=[{"code": "600036", "name": "招商银行", "action": "BUY", "score": 7.2}],
        )

        engine = BacktestEngine(BacktestConfig(), history_conn=conn)
        engine._bars = {
            "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
        }
        fallback_market = MarketState(signal=MarketSignal.RED, multiplier=0.0)

        replay = engine._mirror_replay_for_date("2026-01-05", fallback_market)
    finally:
        conn.close()

    assert replay is not None
    assert replay["source"] == "history_mirror"
    assert replay["history_group_id"] == "hist_20260105_screener"
    assert replay["market"].signal == MarketSignal.GREEN
    score, intent = replay["intents"][0]
    assert score.code == "600036"
    assert score.total == 7.2
    assert intent.action.value == "BUY"
