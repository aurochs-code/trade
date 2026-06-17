"""Backtest risk management behavior."""

from pathlib import Path

import pandas as pd
import yaml

import astock_trading.market.adapters as market_adapters
import astock_trading.backtest.engine as backtest_engine_module
from astock_trading.backtest.engine import (
    BacktestConfig,
    BacktestEngine,
    PendingBuyOrder,
    Position,
    _date_ranges,
    _financial_periods,
    _is_market_reduce_signal,
    _score_weights_for_mode,
    load_config,
    run_backtest,
)
from astock_trading.market.store import MarketStore
from astock_trading.strategy.models import Action, DecisionIntent, MarketSignal, MarketState, ScoreResult


def _strategy_config(path: str) -> dict:
    repo_root = Path(__file__).resolve().parents[3]
    return yaml.safe_load((repo_root / path).read_text(encoding="utf-8"))


def test_risk_check_triggers_trailing_stop_from_high_watermark():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            stop_loss=0.08,
            trailing_stop=0.10,
            time_stop_days=30,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]
    engine._sorted_dates = dates
    engine._cash = 90000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 12.0, 10.8, 10.4],
            "最高": [10.1, 12.2, 10.9, 10.5],
            "最低": [9.9, 11.8, 10.4, 10.2],
            "收盘": [10.0, 12.0, 10.5, 10.3],
            "成交量": [1000000, 1000000, 1000000, 1000000],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=12.0,
        )
    }

    engine._risk_check(dates[2], 2)

    assert "002138" in engine._positions
    assert engine._positions["002138"].pending_exit_reason == "追踪止损"
    assert engine._trades == []

    engine._risk_check(dates[3], 3)

    assert "002138" not in engine._positions
    assert engine._trades[-1]["reason"] == "追踪止损"
    assert engine._trades[-1]["trigger_date"] == dates[2]
    assert engine._trades[-1]["execution_date"] == dates[3]
    assert engine._trades[-1]["trade_cost"]["total_cost"] > 0


def test_load_data_uses_single_bulk_market_bar_query_when_cache_only():
    def frame(rows):
        return pd.DataFrame(rows)

    class FakeStore:
        def __init__(self):
            self.calls = []

        def get_price_bars_bulk(self, symbols, **kwargs):
            self.calls.append((list(symbols), dict(kwargs)))
            rows = {
                "000001": frame([
                    {"日期": "2026-01-01", "开盘": 10, "最高": 10.5, "最低": 9.8, "收盘": 10.1, "成交量": 100, "成交额": 1000, "涨跌幅": 0.0},
                    {"日期": "2026-01-02", "开盘": 10.1, "最高": 10.6, "最低": 10.0, "收盘": 10.4, "成交量": 100, "成交额": 1000, "涨跌幅": 2.97},
                    {"日期": "2026-01-05", "开盘": 10.4, "最高": 10.7, "最低": 10.2, "收盘": 10.6, "成交量": 100, "成交额": 1000, "涨跌幅": 1.92},
                ]),
                "600036": frame([
                    {"日期": "2026-01-01", "开盘": 35, "最高": 36, "最低": 34.5, "收盘": 35.2, "成交量": 200, "成交额": 2000, "涨跌幅": 0.0},
                    {"日期": "2026-01-02", "开盘": 35.2, "最高": 36.5, "最低": 35.0, "收盘": 36.0, "成交量": 200, "成交额": 2000, "涨跌幅": 2.27},
                    {"日期": "2026-01-05", "开盘": 36.0, "最高": 36.8, "最低": 35.8, "收盘": 36.4, "成交量": 200, "成交额": 2000, "涨跌幅": 1.11},
                ]),
            }
            return {symbol: rows.get(symbol, pd.DataFrame()) for symbol in symbols}

    engine = BacktestEngine(
        BacktestConfig(use_market_bars=True, load_financials=False),
    )
    fake_store = FakeStore()
    engine._market_store = fake_store

    result = engine.load_data(["600036"], "2026-01-02", "2026-01-05", "2026-01-01")

    assert result == {"loaded": 1, "trading_days": 2}
    assert len(fake_store.calls) == 1
    assert sorted(fake_store.calls[0][0]) == ["000001", "600036"]
    assert fake_store.calls[0][1]["adjustflag"] == "2"
    assert engine._bars["600036"]["名称"].tolist() == ["600036", "600036", "600036"]


def test_load_data_accepts_partial_market_cache_without_remote_fetch(monkeypatch):
    class FailingAdapter:
        async def get_kline(self, *args, **kwargs):
            raise AssertionError("cache-only 模式不应回退远程 K 线")

    class FakeStore:
        def get_price_bars_bulk(self, symbols, **kwargs):
            return {
                "000001": pd.DataFrame([
                    {"日期": "2026-01-01", "开盘": 10, "最高": 10.5, "最低": 9.8, "收盘": 10.1, "成交量": 100, "成交额": 1000, "涨跌幅": 0.0},
                    {"日期": "2026-01-02", "开盘": 10.1, "最高": 10.6, "最低": 10.0, "收盘": 10.4, "成交量": 100, "成交额": 1000, "涨跌幅": 2.97},
                    {"日期": "2026-01-05", "开盘": 10.4, "最高": 10.7, "最低": 10.2, "收盘": 10.6, "成交量": 100, "成交额": 1000, "涨跌幅": 1.92},
                ]),
                "600036": pd.DataFrame([
                    {"日期": "2026-01-01", "开盘": 35, "最高": 36, "最低": 34.5, "收盘": 35.2, "成交量": 200, "成交额": 2000, "涨跌幅": 0.0},
                    {"日期": "2026-01-02", "开盘": 35.2, "最高": 36.5, "最低": 35.0, "收盘": 36.0, "成交量": 200, "成交额": 2000, "涨跌幅": 2.27},
                ]),
            }

    monkeypatch.setattr(market_adapters, "BaoStockMarketAdapter", FailingAdapter)
    engine = BacktestEngine(
        BacktestConfig(use_market_bars=True, hydrate_market_bars=False, load_financials=False),
    )
    engine._market_store = FakeStore()

    result = engine.load_data(["600036"], "2026-01-02", "2026-01-05", "2026-01-01")

    assert result == {"loaded": 1, "trading_days": 2}
    assert engine._bars["600036"]["日期"].tolist() == ["2026-01-01", "2026-01-02"]


def test_candidate_codes_for_date_uses_recent_discovery_window():
    engine = BacktestEngine(
        BacktestConfig(require_reachable_candidate_for_buy=True, reachable_lookback_days=5),
    )
    engine._bars = {
        "600036": pd.DataFrame(),
        "300750": pd.DataFrame(),
        "002594": pd.DataFrame(),
    }
    engine._recent_discoveries = {
        "600036": {"last_seen_date": "2026-06-10", "sources": ["pool"]},
        "300750": {"last_seen_date": "2026-06-01", "sources": ["pool"]},
        "999999": {"last_seen_date": "2026-06-10", "sources": ["pool"]},
    }

    assert engine._candidate_codes_for_date("2026-06-15") == ["600036"]


def test_risk_check_defers_entry_day_exit_until_next_trading_day():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            stop_loss=0.08,
            trailing_stop=0.50,
            time_stop_days=30,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06"]
    engine._sorted_dates = dates
    engine._cash = 90000.0
    engine._bars = {
            "002138": pd.DataFrame({
                "日期": dates,
                "开盘": [10.0, 8.9, 8.7],
                "最高": [10.1, 9.0, 8.9],
                "最低": [9.0, 8.8, 8.6],
                "收盘": [9.0, 8.8, 8.7],
                "涨跌幅": [-10.0, -2.22, -1.14],
                "成交量": [1000000, 1000000, 1000000],
            })
        }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=10.0,
        )
    }

    engine._risk_check(dates[0], 0)

    assert "002138" in engine._positions
    assert engine._trades == []
    assert engine._positions["002138"].pending_exit_reason == "止损"
    assert engine._execution_constraints["sell_orders_pending"] == 1

    engine._risk_check(dates[1], 1)

    assert "002138" not in engine._positions
    assert engine._trades[-1]["date"] == dates[1]
    assert engine._trades[-1]["trigger_date"] == dates[0]
    assert engine._trades[-1]["execution_date"] == dates[1]
    assert engine._trades[-1]["reason"] == "止损"


def test_risk_check_keeps_position_when_exit_hits_locked_limit_down():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            stop_loss=0.08,
            trailing_stop=0.50,
            time_stop_days=30,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06"]
    engine._sorted_dates = dates
    engine._cash = 90000.0
    engine._bars = {
            "600036": pd.DataFrame({
                "日期": dates,
                "开盘": [10.0, 9.5, 8.1],
                "最高": [10.0, 9.6, 8.2],
                "最低": [10.0, 9.0, 8.1],
                "收盘": [10.0, 9.0, 8.1],
                "涨跌幅": [0.0, -10.0, -10.0],
                "成交量": [1000000, 1000000, 1000000],
            })
        }
    engine._positions = {
        "600036": Position(
            code="600036",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=10.0,
        )
    }

    engine._risk_check(dates[1], 1)
    assert engine._positions["600036"].pending_exit_reason == "止损"

    engine._risk_check(dates[2], 2)

    assert "600036" in engine._positions
    assert engine._trades == []
    assert engine._execution_constraints["sell_untradable"] == 1
    assert engine._execution_constraints["untradable_reasons"]["limit_down_locked"] == 1


def test_backtest_tradeability_blocks_locked_limit_up_buy():
    engine = BacktestEngine(BacktestConfig())
    df = pd.DataFrame({
        "日期": ["2026-01-02", "2026-01-05"],
        "开盘": [10.0, 11.0],
        "最高": [10.0, 11.0],
        "最低": [10.0, 11.0],
        "收盘": [10.0, 11.0],
        "涨跌幅": [0.0, 10.0],
        "成交量": [1000000, 1000000],
    })

    status = engine._tradeability_status(df, "2026-01-05", side="buy", code="600036")

    assert status == {"tradable": False, "reason": "limit_up_locked"}


def test_backtest_pending_buy_executes_next_trading_day_open():
    engine = BacktestEngine(BacktestConfig(initial_cash=100000.0, single_max_pct=0.20, total_max_pct=0.60))
    dates = ["2026-01-02", "2026-01-05"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 10.8],
            "最高": [10.2, 11.0],
            "最低": [9.9, 10.6],
            "收盘": [10.0, 10.9],
            "成交量": [1000000, 1000000],
        })
    }
    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=7.0,
        entry_signal=True,
        primary_strategy_route="pullback_to_ma20",
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.20,
        market_signal=MarketSignal.GREEN,
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    engine._pending_buy_orders.append(PendingBuyOrder(
        signal_date=dates[0],
        execution_date=dates[1],
        score=score,
        intent=intent,
        market=market,
        position_pct=0.20,
    ))

    engine._execute_pending_buy_orders(dates[0])

    assert "002138" not in engine._positions

    engine._execute_pending_buy_orders(dates[1])

    assert "002138" in engine._positions
    trade = engine._trades[-1]
    assert trade["side"] == "buy"
    assert trade["signal_date"] == dates[0]
    assert trade["execution_date"] == dates[1]
    assert trade["signal_price"] == 10.8
    assert trade["price"] > 10.8


def test_backtest_pnl_track_prevents_raw_ex_rights_false_stop():
    engine = BacktestEngine(BacktestConfig(adjustflag="3", stop_loss=0.08, trailing_stop=0.20, time_stop_days=30))
    dates = ["2026-01-02", "2026-01-05"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 5.1],
            "最高": [10.2, 5.2],
            "最低": [9.9, 5.0],
            "收盘": [10.0, 5.1],
            "成交量": [1000000, 1000000],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 10.2],
            "最高": [10.2, 10.4],
            "最低": [9.9, 10.0],
            "收盘": [10.0, 10.2],
            "成交量": [1000000, 1000000],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=10.0,
            pnl_units=1000.0,
            pnl_entry_price=10.0,
            pnl_high_water=10.0,
        )
    }

    engine._risk_check(dates[1], 1)

    assert "002138" in engine._positions
    assert engine._positions["002138"].pending_exit_reason == ""
    assert engine._trades == []


def test_backtest_risk_check_skips_when_required_pnl_price_missing():
    engine = BacktestEngine(
        BacktestConfig(
            adjustflag="3",
            pnl_adjustflag="1",
            stop_loss=0.08,
            trailing_stop=0.20,
            time_stop_days=30,
        )
    )
    dates = ["2026-01-02", "2026-01-05"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 5.1],
            "最高": [10.2, 5.2],
            "最低": [9.9, 5.0],
            "收盘": [10.0, 5.1],
            "成交量": [1000000, 1000000],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": [dates[0]],
            "开盘": [10.0],
            "最高": [10.2],
            "最低": [9.9],
            "收盘": [10.0],
            "成交量": [1000000],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=10.0,
            pnl_units=1000.0,
            pnl_entry_price=10.0,
            pnl_high_water=10.0,
        )
    }

    engine._risk_check(dates[1], 1)

    assert engine._positions["002138"].pending_exit_reason == ""
    assert engine._execution_constraints["pnl_price_missing_risk_skips"] == 1
    assert engine._trades == []


def test_backtest_close_position_uses_cash_price_not_adjusted_pnl_value():
    engine = BacktestEngine(
        BacktestConfig(
            adjustflag="3",
            pnl_adjustflag="1",
            commission_bps=0,
            min_commission=0,
            stamp_tax_bps=0,
            transfer_fee_bps=0,
            slippage_bps=0,
        )
    )
    dates = ["2026-01-02", "2026-06-01"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 10.0],
            "收盘": [10.0, 10.0],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [12.0, 13.2],
            "收盘": [12.0, 13.2],
        })
    }
    engine._cash = 0.0
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=10.0,
            pnl_units=10000.0 / 12.0,
            pnl_entry_price=12.0,
            pnl_high_water=12.0,
            cost_basis=10000.0,
        )
    }

    trade = engine._close_position(
        trade_date=dates[1],
        code="002138",
        price=10.0,
        shares=1000,
        reason="到期",
        score=0.0,
    )

    assert trade["gross_proceeds"] == 10000.0
    assert trade["cost_basis"] == 10000.0
    assert trade["pnl"] == 0.0
    assert trade["return_pct"] == 0.0
    assert engine._cash == 10000.0


def test_backtest_pending_buy_keeps_pnl_units_equal_real_shares():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.10,
            total_max_pct=0.60,
            adjustflag="3",
            pnl_adjustflag="1",
            commission_bps=0,
            min_commission=0,
            stamp_tax_bps=0,
            transfer_fee_bps=0,
            slippage_bps=0,
        )
    )
    dates = ["2026-01-02", "2026-01-05"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 10.0],
            "最高": [10.0, 10.0],
            "最低": [10.0, 10.0],
            "收盘": [10.0, 10.0],
            "成交量": [1000000, 1000000],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [12.0, 12.0],
            "收盘": [12.0, 12.0],
        })
    }
    score = ScoreResult(code="002138", name="双环传动", total=7.0, entry_signal=True)
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        market_signal=MarketSignal.GREEN,
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    engine._pending_buy_orders.append(PendingBuyOrder(
        signal_date=dates[0],
        execution_date=dates[1],
        score=score,
        intent=intent,
        market=market,
        position_pct=0.10,
    ))

    engine._execute_pending_buy_orders(dates[1])

    position = engine._positions["002138"]
    assert position.shares == 1000
    assert position.pnl_units == 1000.0
    assert position.pnl_entry_price == 12.0
    assert position.cost_basis == 10000.0


def test_backtest_forward_returns_use_pnl_track_not_raw_ex_rights():
    engine = BacktestEngine(BacktestConfig(adjustflag="3"))
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 5.0, 5.0, 5.0, 5.0, 5.0],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.4, 10.6, 10.8, 11.0],
        })
    }

    assert engine._forward_returns("002138", dates[0], horizons=(5,)) == {"5d": 0.1}


def test_backtest_pending_buy_orders_count_toward_queue_capacity():
    engine = BacktestEngine(BacktestConfig(daily_max_buys=2, weekly_max=2))
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(code="002138", name="双环传动", total=7.0)
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        market_signal=MarketSignal.GREEN,
    )
    engine._pending_buy_orders.append(PendingBuyOrder(
        signal_date="2026-01-02",
        execution_date="2026-01-05",
        score=score,
        intent=intent,
        market=market,
        position_pct=0.2,
    ))

    assert engine._pending_buy_order_count(execution_date="2026-01-05", include_scale_in=False) == 1
    assert (
        engine._weekly_max_for_market(market)
        - engine._weekly_buy_count
        - engine._pending_buy_order_count(include_scale_in=False)
    ) == 1


def test_backtest_trade_cost_model_calculates_fees_by_side():
    engine = BacktestEngine(
        BacktestConfig(
            commission_bps=2.5,
            min_commission=0.0,
            stamp_tax_bps=5.0,
            transfer_fee_bps=0.1,
            slippage_bps=0.0,
        )
    )

    buy = engine._trade_costs_for_order(side="buy", price=10.0, shares=100)
    sell = engine._trade_costs_for_order(side="sell", price=10.0, shares=100)

    assert buy == {
        "notional": 1000.0,
        "commission": 0.25,
        "stamp_tax": 0.0,
        "transfer_fee": 0.01,
        "slippage_cost": 0.0,
        "fee_total": 0.26,
        "total_cost": 0.26,
        "cash_effect": 1000.26,
        "execution_price": 10.0,
    }
    assert sell == {
        "notional": 1000.0,
        "commission": 0.25,
        "stamp_tax": 0.5,
        "transfer_fee": 0.01,
        "slippage_cost": 0.0,
        "fee_total": 0.76,
        "total_cost": 0.76,
        "cash_effect": 999.24,
        "execution_price": 10.0,
    }


def test_backtest_trade_cost_model_does_not_double_count_slippage_cash_effect():
    engine = BacktestEngine(
        BacktestConfig(
            commission_bps=0.0,
            min_commission=0.0,
            stamp_tax_bps=0.0,
            transfer_fee_bps=0.0,
            slippage_bps=10.0,
        )
    )

    buy = engine._trade_costs_for_order(side="buy", price=10.0, shares=100)
    sell = engine._trade_costs_for_order(side="sell", price=10.0, shares=100)

    assert buy["execution_price"] == 10.01
    assert buy["notional"] == 1001.0
    assert buy["slippage_cost"] == 1.0
    assert buy["total_cost"] == 1.0
    assert buy["cash_effect"] == 1001.0
    assert sell["execution_price"] == 9.99
    assert sell["notional"] == 999.0
    assert sell["slippage_cost"] == 1.0
    assert sell["total_cost"] == 1.0
    assert sell["cash_effect"] == 999.0


def test_backtest_report_exposes_realistic_execution_constraints_and_costs():
    engine = BacktestEngine(BacktestConfig(include_signal_alpha=False))

    report = engine._build_report()

    assert report["execution_semantics"]["t_plus_one"] is True
    assert report["execution_semantics"]["signal_execution_lag"] == "next_trading_day_open"
    assert report["execution_semantics"]["limit_price_model"] == "execution_price_near_limit_blocked"
    assert report["cost_model"]["commission_bps"] == 2.5
    assert report["cost_model"]["stamp_tax_bps"] == 5.0
    assert report["execution_constraints"]["t_plus_one_blocked_sells"] == 0


def test_backtest_report_exposes_pnl_bar_coverage_gaps():
    engine = BacktestEngine(BacktestConfig(adjustflag="3", pnl_adjustflag="1", include_signal_alpha=False))
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": ["2026-01-02", "2026-01-05"],
            "收盘": [10.0, 5.1],
        })
    }
    engine._pnl_bars = {
        "002138": pd.DataFrame({
            "日期": ["2026-01-02"],
            "收盘": [10.0],
        })
    }

    coverage = engine._build_report()["data_coverage"]["pnl_bar_coverage"]

    assert coverage["enabled"] is True
    assert coverage["checked_codes"] == 1
    assert coverage["pnl_loaded_codes"] == 1
    assert coverage["missing_code_count"] == 1
    assert coverage["missing_date_count"] == 1
    assert coverage["sample_codes"]["002138"]["sample_dates"] == ["2026-01-05"]


def test_market_reduction_depends_on_signal_not_multiplier():
    assert _is_market_reduce_signal(MarketState(signal=MarketSignal.RED, multiplier=0.3)) is True
    assert _is_market_reduce_signal(MarketState(signal=MarketSignal.CLEAR, multiplier=0.0)) is True
    assert _is_market_reduce_signal(MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)) is False


def test_backtest_can_disable_market_reduce_sell_for_research():
    engine = BacktestEngine(BacktestConfig(disable_market_reduce_sell=True))

    assert engine._should_market_reduce_position(MarketState(signal=MarketSignal.RED, multiplier=0.3)) is False


def test_backtest_market_reduce_respects_route_policy_exemption():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.RED, multiplier=0.3)

    normal = Position(
        code="002138",
        shares=100,
        entry_price=10.0,
        entry_date="2026-01-01",
        high_water=10.0,
    )
    exempt = Position(
        code="002139",
        shares=100,
        entry_price=10.0,
        entry_date="2026-01-01",
        high_water=10.0,
        market_reduce_exempt=True,
    )

    assert engine._should_reduce_position_for_market(normal, market) is True
    assert engine._should_reduce_position_for_market(exempt, market) is False


def test_backtest_watch_loss_cooldown_blocks_watch_but_not_buy():
    engine = BacktestEngine(
        BacktestConfig(
            watch_loss_cooldown_days=3,
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
        )
    )
    engine._current_date_index = 2
    engine._register_loss_cooldown({"side": "sell", "pnl": -100.0, "reason": "止损"}, date_index=2)
    watch_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )
    buy_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=6.8,
        score=6.8,
        position_pct=0.22,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        watch_intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == {"executable": False, "reason": "watch_loss_cooldown"}
    assert engine._intent_execution_status(
        buy_intent,
        "002138",
        "short_continuation",
        score_total=6.8,
        market=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == {"executable": True, "reason": "buy"}


def test_backtest_watch_loss_cooldown_expires_by_trading_index():
    engine = BacktestEngine(
        BacktestConfig(
            watch_loss_cooldown_days=2,
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
        )
    )
    engine._current_date_index = 5
    engine._register_loss_cooldown({"side": "sell", "pnl": -100.0, "reason": "大盘RED减仓"}, date_index=2)
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_backtest_watch_loss_cooldown_can_be_limited_to_market_phases():
    engine = BacktestEngine(
        BacktestConfig(
            watch_loss_cooldown_days=5,
            watch_loss_cooldown_phase_buckets=("below_ma20_slope_up",),
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
        )
    )
    engine._current_date_index = 3
    engine._register_loss_cooldown({"side": "sell", "pnl": -100.0, "reason": "止损"}, date_index=2)
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": -1.0,
                "index_ma20_slope_5d_pct": 0.8,
                "ma120": 3050.0,
                "above_ma120": False,
                "index_ma120_slope_20d_pct": -1.2,
            },
        ),
    ) == {"executable": False, "reason": "watch_loss_cooldown"}
    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={"index_ma20_deviation_pct": 4.0, "index_ma20_slope_5d_pct": 0.8},
        ),
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_backtest_watch_trial_can_require_above_ma20_days_for_specific_phase():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
            execute_watch_trial_phase_buckets=("extended_above_ma20_slope_up",),
            execute_watch_trial_min_above_ma20_days=6,
            execute_watch_trial_min_above_ma20_days_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
                "above_ma20_days": 5,
            },
        ),
    ) == {"executable": False, "reason": "watch_above_ma20_days_below_min"}
    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
                "above_ma20_days": 6,
            },
        ),
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_backtest_watch_trial_can_require_above_ma60_for_specific_phase():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
            execute_watch_trial_phase_buckets=("extended_above_ma20_slope_up",),
            execute_watch_trial_require_above_ma60_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "price": 3100.0,
                "ma60": 3200.0,
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
            },
        ),
    ) == {"executable": False, "reason": "watch_above_ma60_required"}
    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "price": 3300.0,
                "ma60": 3200.0,
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
            },
        ),
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_backtest_watch_trial_can_require_above_ma120_for_specific_phase():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
            execute_watch_trial_phase_buckets=("extended_above_ma20_slope_up",),
            execute_watch_trial_require_above_ma120_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "price": 3100.0,
                "ma120": 3200.0,
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
            },
        ),
    ) == {"executable": False, "reason": "watch_above_ma120_required"}
    assert engine._intent_execution_status(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.2,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "price": 3300.0,
                "ma120": 3200.0,
                "index_ma20_deviation_pct": 4.0,
                "index_ma20_slope_5d_pct": 1.0,
            },
        ),
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_runtime_base_config_matches_final_route_candidate_risk_profile():
    for config_path in (
        "config/strategy.yaml",
        "src/astock_trading/templates/config/strategy.yaml",
    ):
        cfg = _strategy_config(config_path)
        final_candidate = cfg["backtest_presets"]["攻_C_recovery_ma120_green_scale04"]
        momentum = cfg["risk"]["momentum"]
        position = cfg["risk"]["position"]

        assert momentum["trailing_stop"] == final_candidate["momentum_trailing_stop"] == 0.16
        assert momentum["time_stop_days"] == final_candidate["momentum_time_stop_days"] == 30
        assert position["single_max"] == final_candidate["single_max_pct"] == 0.22
        assert position["total_max"] == final_candidate["total_max_pct"] == 0.67
        assert position["weekly_max"] == final_candidate["weekly_max"] == 5
        assert position["holding_max"] == final_candidate["holding_max"] == 5


def test_backtest_preset_can_carry_research_execution_fields():
    cfg = load_config("攻_C_phase_filtered_watch08")

    assert cfg.execute_buy_phase_buckets == (
        "extended_above_ma20_slope_up",
        "extended_above_ma20_slope_flat",
        "below_ma20_slope_flat",
        "below_ma20_slope_up",
    )
    assert cfg.execute_watch_trial_pairs == (
        "GREEN:trend_cooling_off",
        "RED:relative_strength_overheat",
        "YELLOW:trend_cooling_off",
    )
    assert cfg.execute_watch_trial_score_min == 0.0
    assert cfg.execute_watch_trial_score_max == 4.5
    assert cfg.execute_watch_trial_position_pct == 0.08
    assert cfg.execute_watch_trial_phase_buckets == (
        "extended_above_ma20_slope_up",
        "extended_above_ma20_slope_flat",
        "below_ma20_slope_flat",
        "below_ma20_slope_up",
    )
    assert cfg.market_multipliers["RED"] == 0.3


def test_backtest_final_candidate_preset_carries_phase_limited_loss_cooldown():
    cfg = load_config("攻_C_phase_filtered_watch08_cooldown20")

    assert cfg.watch_loss_cooldown_days == 20
    assert cfg.watch_loss_cooldown_phase_buckets == (
        "extended_above_ma20_slope_flat",
        "below_ma20_slope_flat",
        "below_ma20_slope_up",
    )
    assert cfg.execute_watch_trial_position_pct == 0.075
    assert cfg.execute_watch_trial_score_max == 4.5
    assert "below_ma20_slope_down" in cfg.execute_watch_trial_phase_buckets
    assert cfg.execute_watch_trial_require_above_ma120_phase_buckets == (
        "extended_above_ma20_slope_up",
        "extended_above_ma20_slope_flat",
    )
    assert cfg.scale_in_enabled is True
    assert cfg.scale_in_profit_threshold == 0.10
    assert cfg.scale_in_step_position_pct == 0.075
    assert cfg.scale_in_max_position_pct == 0.22
    assert cfg.scale_in_max_adds == 2
    assert cfg.scale_in_min_days_between == 5
    assert cfg.scale_in_routes == (
        "short_continuation",
        "volume_breakout",
        "dragon_head",
        "trend_structure_watch",
        "pullback_to_ma20",
        "trend_cooling_off",
        "relative_strength_overheat",
    )
    assert cfg.scale_in_market_signals == ("GREEN", "YELLOW", "RED")
    assert cfg.scale_in_actions == ("BUY", "WATCH", "TRIAL_BUY")
    assert cfg.scale_in_require_entry_signal is False
    assert cfg.scale_in_score_min == 4.0
    assert cfg.scale_in_reset_time_stop is True
    assert cfg.scale_in_aggressive_max_position_pct == 0.30
    assert cfg.scale_in_aggressive_step_position_pct == 0.08
    assert cfg.scale_in_aggressive_market_signals == ("GREEN", "YELLOW")
    assert cfg.scale_in_aggressive_routes == (
        "short_continuation",
        "volume_breakout",
        "dragon_head",
        "trend_structure_watch",
    )
    assert cfg.scale_in_aggressive_phase_buckets == (
        "extended_above_ma20_slope_up",
        "extended_above_ma20_slope_flat",
        "near_ma20_slope_up",
        "below_ma20_slope_up",
    )


def test_backtest_execution_semantics_keeps_scale_in_production_mode():
    engine = BacktestEngine(
        BacktestConfig(
            scale_in_enabled=True,
            scale_in_profit_threshold=0.10,
            scale_in_step_position_pct=0.075,
            scale_in_max_position_pct=0.22,
            scale_in_max_adds=1,
            scale_in_market_signals=("GREEN", "YELLOW"),
            scale_in_routes=("trend_cooling_off",),
            scale_in_actions=("WATCH", "TRIAL_BUY"),
            scale_in_require_entry_signal=False,
            scale_in_aggressive_max_position_pct=0.30,
            scale_in_aggressive_step_position_pct=0.08,
            scale_in_aggressive_market_signals=("GREEN", "YELLOW"),
            scale_in_aggressive_routes=("short_continuation",),
            scale_in_aggressive_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )

    semantics = engine._execution_semantics()

    assert semantics["mode"] == "production_buy_only"
    assert semantics["buy_only"] is True
    assert semantics["scale_in_enabled"] is True
    assert semantics["scale_in_routes"] == ["trend_cooling_off"]
    assert semantics["scale_in_actions"] == ["WATCH", "TRIAL_BUY"]
    assert semantics["scale_in_require_entry_signal"] is False
    assert semantics["scale_in_aggressive_max_position_pct"] == 0.30
    assert semantics["scale_in_aggressive_step_position_pct"] == 0.08
    assert semantics["scale_in_aggressive_markets"] == ["GREEN", "YELLOW"]
    assert semantics["scale_in_aggressive_routes"] == ["short_continuation"]
    assert semantics["scale_in_aggressive_phases"] == ["extended_above_ma20_slope_up"]


def test_backtest_config_loads_production_route_scale_in_preset_without_research_what_if():
    cfg = load_config("攻_C_production_route_scale_in")

    assert cfg.scale_in_enabled is True
    assert cfg.scale_in_aggressive_max_position_pct == 0.30
    assert cfg.route_execution_policy["GREEN:trend_cooling_off"]["actions"] == ["BUY"]
    assert cfg.route_execution_policy["RED:relative_strength_overheat"]["allow_market_blocked"] is True
    assert cfg.execute_watch_trial_pairs == ()
    assert cfg.execute_watch_trial_routes == ()
    assert cfg.execute_watch_trial_market_signals == ()
    assert cfg.watch_loss_cooldown_days == 0

    engine = BacktestEngine(cfg)
    semantics = engine._execution_semantics()

    assert semantics["mode"] == "production_buy_only"
    assert semantics["watch_trial_enabled"] is False
    assert semantics["scale_in_enabled"] is True


def test_backtest_buy_uses_decision_position_pct():
    engine = BacktestEngine(BacktestConfig(single_max_pct=0.20))
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.10,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )

    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.YELLOW, multiplier=0.5),
    ) == 0.10


def test_backtest_scale_in_budget_only_buys_delta_to_target_pct():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            total_max_pct=0.67,
        )
    )
    trade_date = "2026-01-05"
    engine._cash = 85000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": [trade_date],
            "收盘": [15.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=trade_date,
            high_water=15.0,
            position_pct=0.15,
        )
    }

    assert engine._allocation_budget_to_target_position(trade_date, "002138", 0.22) == 7000.0
    assert engine._allocation_budget_to_target_position(trade_date, "002138", 0.15) == 0.0


def test_backtest_scale_in_adds_to_profitable_trend_position():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.22,
            total_max_pct=0.67,
            scale_in_enabled=True,
            scale_in_profit_threshold=0.10,
            scale_in_step_position_pct=0.075,
            scale_in_max_position_pct=0.22,
            scale_in_max_adds=2,
            scale_in_min_days_between=1,
            scale_in_routes=("short_continuation",),
            scale_in_market_signals=("GREEN",),
            scale_in_score_min=5.0,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06"]
    engine._sorted_dates = dates
    engine._cash = 95000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 12.0, 12.0],
            "收盘": [10.0, 12.0, 12.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=500,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=12.0,
            position_pct=0.075,
        )
    }
    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.6,
        entry_signal=True,
        primary_strategy_route="short_continuation",
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=6.6,
        score=6.6,
        position_pct=0.22,
        market_signal=MarketSignal.GREEN,
    )

    engine._scale_in_positions(
        trade_date=dates[1],
        day_index=1,
        intents=[(score, intent)],
        market=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    )

    assert len(engine._pending_buy_orders) == 1
    assert engine._pending_buy_orders[0].signal_date == dates[1]
    assert engine._pending_buy_orders[0].execution_date == dates[2]

    engine._execute_pending_buy_orders(dates[2])

    pos = engine._positions["002138"]
    assert pos.shares == 1200
    assert round(pos.entry_price, 4) == 11.1702
    assert pos.position_pct == 0.15
    assert pos.add_count == 1
    assert pos.last_add_date == dates[2]
    assert engine._trades[-1]["source_action"] == "SCALE_IN"
    assert engine._trades[-1]["reason"] == "趋势加仓"
    assert engine._trades[-1]["position_pct"] == 0.15
    assert engine._trades[-1]["signal_date"] == dates[1]
    assert engine._trades[-1]["execution_date"] == dates[2]


def test_backtest_scale_in_rejects_weak_or_disallowed_confirmation():
    engine = BacktestEngine(
        BacktestConfig(
            scale_in_enabled=True,
            scale_in_profit_threshold=0.10,
            scale_in_routes=("short_continuation",),
            scale_in_market_signals=("GREEN",),
            scale_in_score_min=5.0,
        )
    )
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=500,
            entry_price=10.0,
            entry_date="2026-01-02",
            high_water=12.0,
            position_pct=0.075,
        )
    }
    weak_score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.6,
        entry_signal=True,
        primary_strategy_route="trend_cooling_off",
    )

    assert engine._scale_in_execution_status(
        weak_score,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
        price=12.0,
        day_index=1,
    ) == {"executable": False, "reason": "scale_in_route_blocked"}


def test_backtest_scale_in_can_use_trial_buy_confirmation_without_new_entry_signal():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.22,
            total_max_pct=0.67,
            scale_in_enabled=True,
            scale_in_profit_threshold=0.10,
            scale_in_step_position_pct=0.075,
            scale_in_max_position_pct=0.22,
            scale_in_max_adds=1,
            scale_in_min_days_between=1,
            scale_in_routes=("trend_cooling_off",),
            scale_in_market_signals=("GREEN",),
            scale_in_actions=("TRIAL_BUY",),
            scale_in_require_entry_signal=False,
            scale_in_score_min=5.0,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06"]
    engine._sorted_dates = dates
    engine._cash = 95000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "开盘": [10.0, 12.0, 12.0],
            "收盘": [10.0, 12.0, 12.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=500,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=12.0,
            position_pct=0.075,
        )
    }
    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=5.6,
        entry_signal=False,
        primary_strategy_route="trend_cooling_off",
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.TRIAL_BUY,
        confidence=5.6,
        score=5.6,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
    )

    engine._scale_in_positions(
        trade_date=dates[1],
        day_index=1,
        intents=[(score, intent)],
        market=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    )

    assert len(engine._pending_buy_orders) == 1
    engine._execute_pending_buy_orders(dates[2])

    assert engine._positions["002138"].add_count == 1
    assert engine._trades[-1]["source_action"] == "SCALE_IN"


def test_backtest_scale_in_dynamic_aggressive_target_for_strong_market_route():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.22,
            total_max_pct=0.67,
            scale_in_enabled=True,
            scale_in_step_position_pct=0.075,
            scale_in_max_position_pct=0.22,
            scale_in_aggressive_step_position_pct=0.08,
            scale_in_aggressive_max_position_pct=0.30,
            scale_in_aggressive_market_signals=("GREEN", "YELLOW"),
            scale_in_aggressive_routes=("short_continuation",),
            scale_in_aggressive_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    trade_date = "2026-01-05"
    engine._cash = 76000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": [trade_date],
            "收盘": [12.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=2000,
            entry_price=10.0,
            entry_date=trade_date,
            high_water=12.0,
            position_pct=0.22,
        )
    }
    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.8,
        entry_signal=True,
        primary_strategy_route="short_continuation",
    )
    market = MarketState(
        signal=MarketSignal.GREEN,
        multiplier=1.0,
        detail={
            "price": 3300.0,
            "ma20": 3100.0,
            "index_ma20_deviation_pct": 6.0,
            "index_ma20_slope_5d_pct": 1.0,
        },
    )

    assert engine._scale_in_target_position_pct(trade_date, "002138", score=score, market=market) == 0.30


def test_backtest_scale_in_dynamic_target_keeps_red_or_weak_route_at_base_cap():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.22,
            total_max_pct=0.67,
            scale_in_enabled=True,
            scale_in_step_position_pct=0.075,
            scale_in_max_position_pct=0.22,
            scale_in_aggressive_step_position_pct=0.08,
            scale_in_aggressive_max_position_pct=0.30,
            scale_in_aggressive_market_signals=("GREEN", "YELLOW"),
            scale_in_aggressive_routes=("short_continuation",),
            scale_in_aggressive_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    trade_date = "2026-01-05"
    engine._cash = 76000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": [trade_date],
            "收盘": [12.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=2000,
            entry_price=10.0,
            entry_date=trade_date,
            high_water=12.0,
            position_pct=0.22,
        )
    }
    weak_score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.8,
        entry_signal=False,
        primary_strategy_route="relative_strength_overheat",
    )
    red_market = MarketState(
        signal=MarketSignal.RED,
        multiplier=0.3,
        detail={
            "price": 3000.0,
            "ma20": 3100.0,
            "index_ma20_deviation_pct": -3.0,
            "index_ma20_slope_5d_pct": -1.0,
        },
    )

    assert engine._scale_in_target_position_pct(trade_date, "002138", score=weak_score, market=red_market) == 0.22


def test_backtest_can_counterfactually_execute_red_trial_buy_at_small_size():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_trial_buy_market_signals=("RED",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    assert engine._intent_executable_for_backtest(intent, "002138") is True
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.RED, multiplier=0.3),
    ) == 0.06


def test_backtest_can_filter_counterfactual_trial_buy_by_route():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_trial_buy_market_signals=("RED",),
            execute_trial_buy_routes=("pullback_to_ma20",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    assert engine._intent_executable_for_backtest(intent, "002138", "pullback_to_ma20") is True
    assert engine._intent_executable_for_backtest(intent, "002138", "trend_cooling_off") is False


def test_backtest_can_counterfactually_execute_watch_route_with_score_floor():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_watch_trial_market_signals=("GREEN",),
            execute_watch_trial_routes=("relative_strength_overheat",),
            execute_watch_trial_score_min=6.0,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=6.2,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=5.9,
    ) is False
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "pullback_to_ma20",
        score_total=6.2,
    ) is False
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == 0.20


def test_backtest_can_counterfactually_execute_watch_route_with_score_ceiling():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=4.5,
            execute_watch_trial_score_max=5.0,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.8,
        score=4.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.8,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=5.1,
    ) is False


def test_backtest_can_counterfactually_size_watch_route_with_position_override():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_position_pct=0.08,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.8,
        score=4.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
        "trend_cooling_off",
    ) == 0.08


def test_backtest_can_counterfactually_filter_watch_route_by_market_phase():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:trend_cooling_off",),
            execute_watch_trial_score_min=0.0,
            execute_watch_trial_phase_buckets=("below_ma20_slope_up",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.8,
        score=4.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.8,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": -1.0,
                "index_ma20_slope_5d_pct": 0.8,
                "ma120": 3050.0,
                "above_ma120": False,
                "index_ma120_slope_20d_pct": -1.2,
            },
        ),
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "trend_cooling_off",
        score_total=4.8,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={"index_ma20_deviation_pct": 4.0, "index_ma20_slope_5d_pct": -0.8},
        ),
    ) is False


def test_backtest_buy_trade_record_includes_market_phase_context():
    engine = BacktestEngine(BacktestConfig())
    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=4.2,
        primary_strategy_route="trend_cooling_off",
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=4.2,
        score=4.2,
        market_signal=MarketSignal.GREEN,
    )

    record = engine._buy_trade_record(
        trade_date="2026-01-02",
        score=score,
        intent=intent,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": -1.0,
                "index_ma20_slope_5d_pct": 0.8,
                "ma120": 3050.0,
                "above_ma120": False,
                "index_ma120_slope_20d_pct": -1.2,
            },
        ),
        price=10.0,
        shares=100,
        position_pct=0.09,
    )

    assert record["market_signal"] == "GREEN"
    assert record["market_phase_bucket"] == "below_ma20_slope_up"
    assert record["market_context"]["index_ma20_deviation_pct"] == -1.0
    assert record["market_context"]["ma120"] == 3050.0
    assert record["market_context"]["above_ma120"] is False
    assert record["market_context"]["index_ma120_slope_20d_pct"] == -1.2
    assert record["source_route"] == "trend_cooling_off"


def test_backtest_can_counterfactually_filter_buy_by_market_phase():
    engine = BacktestEngine(
        BacktestConfig(
            execute_buy_phase_buckets=("extended_above_ma20_slope_up",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=6.8,
        score=6.8,
        position_pct=0.22,
        market_signal=MarketSignal.GREEN,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "short_continuation",
        score_total=6.8,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={"index_ma20_deviation_pct": 3.5, "index_ma20_slope_5d_pct": 1.0},
        ),
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "pullback_to_ma20",
        score_total=6.8,
        market=MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={"index_ma20_deviation_pct": 1.0, "index_ma20_slope_5d_pct": -0.8},
        ),
    ) is False


def test_backtest_can_filter_counterfactual_watch_route_by_market_route_pair():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_watch_trial_pairs=("GREEN:relative_strength_overheat",),
            execute_watch_trial_score_min=5.0,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.3,
        score=5.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=5.3,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "pullback_to_ma20",
        score_total=5.3,
    ) is False
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == 0.20


def test_score_weights_for_tech_fundamental_mode_rescales_to_full_score_budget():
    weights = _score_weights_for_mode(
        {"technical": 3.0, "fundamental": 2.0, "flow": 2.0, "sentiment": 3.0},
        "tech_fundamental",
    )

    assert weights == {
        "technical": 6.0,
        "fundamental": 4.0,
        "flow": 0.0,
        "sentiment": 0.0,
    }


def test_backtest_config_loads_strategy_overlays_and_score_adjustments():
    cfg = load_config("aggressive_score_capture")

    assert cfg.market_regime_overlays["YELLOW"]["allow_trial_buy"] is False
    assert cfg.market_regime_overlays["YELLOW"]["buy_threshold"] == 9.9
    assert cfg.market_regime_overlays["RED"]["allow_trial_buy"] is True
    assert cfg.score_adjustments["tech_flow_correlation"]["enabled"] is True


def test_backtest_config_allows_preset_market_regime_overlay_override():
    cfg = load_config("攻_C_recovery_ma120_no_scale")

    assert cfg.market_regime_overlays["YELLOW"]["buy_threshold"] == 9.9
    assert cfg.market_regime_overlays["YELLOW"]["allow_trial_buy"] is False
    assert cfg.market_regime_overlays["RED"]["buy_threshold"] == 7.0


def test_backtest_config_loads_recovery_ma120_green_scale04_candidate():
    cfg = load_config("攻_C_recovery_ma120_green_scale04")

    assert cfg.market_regime_overlays["YELLOW"]["buy_threshold"] == 9.9
    assert cfg.route_execution_policy["YELLOW:relative_strength_overheat"]["require_above_ma120"] is True
    assert cfg.trailing_stop == 0.16
    assert cfg.scale_in_enabled is True
    assert cfg.scale_in_step_position_pct == 0.04
    assert cfg.scale_in_market_signals == ("GREEN",)
    assert cfg.scale_in_aggressive_max_position_pct == 0.30
    assert cfg.scale_in_aggressive_step_position_pct == 0.08
    assert "relative_strength_overheat" not in cfg.scale_in_routes


def test_backtest_config_loads_position_limits_from_preset():
    cfg = load_config("攻_C_route_policy_v3_weekly5")

    assert cfg.single_max_pct == 0.22
    assert cfg.total_max_pct == 0.67
    assert cfg.weekly_max == 5


def test_backtest_config_keeps_weekly5_without_market_specific_override():
    cfg = load_config("攻_C_route_policy_v3_weekly5")

    assert cfg.weekly_max == 5
    assert cfg.weekly_max_by_market == {}


def test_backtest_config_loads_route_execution_policy_from_preset():
    cfg = load_config("攻_C_route_policy_v3")

    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["score_min"] == 6.0
    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["position_pct"] == 0.11
    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["priority"] == 35
    assert cfg.daily_max_buys == 3
    assert cfg.holding_max == 5


def test_backtest_config_loads_route_execution_policy_v3_from_preset():
    cfg = load_config("攻_C_route_policy_v3")

    assert cfg.route_execution_policy["GREEN:dragon_head"]["priority"] == 85
    assert cfg.route_execution_policy["YELLOW:pullback_to_ma20"]["position_pct"] == 0.11
    assert cfg.route_execution_policy["YELLOW:volume_breakout"]["priority"] == 80
    assert cfg.route_execution_policy["RED:volume_breakout"]["position_pct"] == 0.066
    red_overheat = cfg.route_execution_policy["RED:relative_strength_overheat"]
    assert red_overheat["position_pct"] == 0.066
    assert red_overheat["score_max"] == 4.5
    assert red_overheat["allow_market_blocked"] is True


def test_backtest_config_loads_route_policy_v3_weekly5_trail18_from_preset():
    cfg = load_config("攻_C_route_policy_v3_weekly5_trail18")

    assert cfg.weekly_max == 5
    assert cfg.daily_max_buys == 3
    assert cfg.holding_max == 5
    assert cfg.trailing_stop == 0.18
    assert cfg.time_stop_days == 30
    assert cfg.route_execution_policy["GREEN:dragon_head"]["priority"] == 85
    assert cfg.route_execution_policy["YELLOW:pullback_to_ma20"]["position_pct"] == 0.11
    assert cfg.route_execution_policy["YELLOW:volume_breakout"]["priority"] == 80
    assert cfg.route_execution_policy["RED:volume_breakout"]["position_pct"] == 0.066


def test_run_backtest_can_override_trailing_stop_without_new_preset(monkeypatch):
    captured = {}

    class FakeEngine:
        def __init__(self, cfg, history_conn=None, market_conn=None):
            captured["cfg"] = cfg

        def load_data(self, code_list, start, end, pre_start):
            captured["pre_start"] = pre_start
            return {}

        def run(self):
            return {"trailing_stop": captured["cfg"].trailing_stop}

    monkeypatch.setattr(backtest_engine_module, "BacktestEngine", FakeEngine)
    monkeypatch.setattr(backtest_engine_module, "_open_history_connection", lambda *args, **kwargs: None)
    monkeypatch.setattr(backtest_engine_module, "_open_market_data_connection", lambda **kwargs: None)

    result = run_backtest(
        "002138",
        "2025-01-01",
        "2025-01-31",
        preset="攻_C_route_policy_v3",
        use_history_mirror=False,
        trailing_stop=0.18,
    )

    assert result["trailing_stop"] == 0.18
    assert captured["pre_start"] == "2024-04-16"


def test_backtest_uses_market_specific_weekly_limit_when_present():
    engine = BacktestEngine(
        BacktestConfig(
            weekly_max=2,
            weekly_max_by_market={"GREEN": 4},
        )
    )

    assert engine._weekly_max_for_market(MarketState(signal=MarketSignal.GREEN, multiplier=1.0)) == 4
    assert engine._weekly_max_for_market(MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)) == 2


def test_backtest_route_policy_defaults_to_buy_only_and_does_not_execute_watch():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "YELLOW:shrink_pullback": {
                    "score_min": 5.8,
                    "position_pct": 0.11,
                    "priority": 35,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.9,
        score=5.9,
        position_pct=0.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)

    status = engine._intent_execution_status(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.9,
    )
    assert status == {"executable": False, "reason": "watch_not_enabled"}
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.9,
    ) is False

    buy_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=6.6,
        score=6.6,
        position_pct=0.22,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    assert engine._intent_execution_status(
        buy_intent,
        "002138",
        "shrink_pullback",
        score_total=6.6,
    ) == {"executable": True, "reason": "buy"}
    assert engine._execution_position_pct(
        buy_intent,
        market,
        route="shrink_pullback",
    ) == 0.11


def test_backtest_route_policy_can_execute_watch_pair_only_when_action_is_explicit():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "YELLOW:shrink_pullback": {
                    "actions": ["WATCH"],
                    "score_min": 5.8,
                    "position_pct": 0.11,
                    "priority": 35,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.9,
        score=5.9,
        position_pct=0.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.9,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.7,
    ) is False
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "short_continuation",
        score_total=6.5,
    ) is False
    assert engine._execution_position_pct(
        intent,
        market,
        route="shrink_pullback",
    ) == 0.11


def test_backtest_explicit_watch_pair_overrides_buy_only_route_policy_for_research():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "YELLOW:shrink_pullback": {
                    "score_min": 6.0,
                    "position_pct": 0.11,
                    "priority": 35,
                }
            },
            execute_watch_trial_pairs=("YELLOW:shrink_pullback",),
            execute_watch_trial_score_min=5.5,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.9,
        score=5.9,
        position_pct=0.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )

    assert engine._intent_execution_status(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.9,
    ) == {"executable": True, "reason": "watch_trial_pair"}


def test_backtest_route_policy_can_execute_trial_buy_with_own_score_floor_and_size():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "GREEN:relative_strength_overheat": {
                    "actions": ["TRIAL_BUY"],
                    "score_min": 6.2,
                    "position_pct": 0.11,
                    "priority": 45,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.3,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.1,
    ) is False
    assert engine._execution_position_pct(
        intent,
        market,
        route="relative_strength_overheat",
    ) == 0.11


def test_backtest_route_policy_defaults_to_buy_only_for_trial_buy():
    engine = BacktestEngine(
        BacktestConfig(
            route_execution_policy={
                "GREEN:relative_strength_overheat": {
                    "score_min": 6.2,
                    "position_pct": 0.11,
                    "priority": 45,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.3,
    ) is False


def test_backtest_route_policy_priority_can_outrank_raw_score():
    engine = BacktestEngine(
        BacktestConfig(
            route_execution_policy={
                "YELLOW:shrink_pullback": {"score_min": 6.0, "priority": 35},
                "YELLOW:short_continuation": {"score_min": 6.0, "priority": 10},
            },
        )
    )
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)
    shrink_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=6.0,
        score=6.0,
        position_pct=0.11,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    short_intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.11,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )

    assert engine._buy_candidate_sort_key(
        shrink_intent,
        "shrink_pullback",
        market,
        score_total=6.0,
    ) > engine._buy_candidate_sort_key(
        short_intent,
        "short_continuation",
        market,
        score_total=7.0,
    )


def test_backtest_route_policy_selects_score_band_for_position_and_priority():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "GREEN:short_continuation": [
                    {"score_min": 4.0, "score_max": 5.0, "position_pct": 0.075, "priority": 55},
                    {"score_min": 6.0, "position_pct": 0.22, "priority": 70},
                ]
            },
        )
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    low_band_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=4.8,
        score=4.8,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )
    high_band_intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.BUY,
        confidence=6.2,
        score=6.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._execution_position_pct(
        low_band_intent,
        market,
        route="short_continuation",
    ) == 0.075
    assert engine._execution_position_pct(
        high_band_intent,
        market,
        route="short_continuation",
    ) == 0.22
    assert engine._buy_candidate_sort_key(
        high_band_intent,
        "short_continuation",
        market,
        score_total=6.2,
    ) > engine._buy_candidate_sort_key(
        low_band_intent,
        "short_continuation",
        market,
        score_total=4.8,
    )


def test_backtest_execution_funnel_reports_zero_position_skip_by_market_route():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.RED, multiplier=0.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=6.7,
        entry_signal=False,
        primary_strategy_route="relative_strength_overheat",
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.WATCH,
        confidence=6.7,
        score=6.7,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    engine._record_execution_funnel_intents("2026-01-02", [(score, intent)], market)
    engine._record_execution_funnel_skip("zero_position_pct", score, intent, market)

    report = engine._build_report()

    assert report["execution_funnel"]["signals_total"] == 1
    assert report["execution_funnel"]["actions"]["WATCH"] == 1
    assert report["execution_funnel"]["skip_reasons"]["zero_position_pct"] == 1
    assert (
        report["execution_funnel"]["by_market_route"]["RED:relative_strength_overheat"]["skip_reasons"][
            "zero_position_pct"
        ]
        == 1
    )


def test_backtest_execution_funnel_reports_decision_reason_counts():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=6.7,
        entry_signal=False,
        primary_strategy_route="relative_strength_overheat",
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.WATCH,
        confidence=6.7,
        score=6.7,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
        notes=["入场信号未触发", "数据质量 degraded 低于要求 ok"],
    )

    engine._record_execution_funnel_intents("2026-01-02", [(score, intent)], market)
    report = engine._build_report()

    assert report["execution_funnel"]["decision_reasons"]["entry_signal_missing"] == 1
    assert report["execution_funnel"]["decision_reasons"]["data_quality_below_min"] == 1
    route_bucket = report["execution_funnel"]["by_market_route"]["GREEN:relative_strength_overheat"]
    assert route_bucket["decision_reasons"]["entry_signal_missing"] == 1
    assert route_bucket["decision_reasons"]["data_quality_below_min"] == 1


def test_backtest_execution_funnel_reports_entry_and_veto_reason_counts():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=0.0,
        entry_signal=True,
        primary_strategy_route="volume_breakout",
        veto_triggered=True,
        hard_veto=["below_ma20"],
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.CLEAR,
        confidence=0.0,
        score=0.0,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
        notes=["一票否决"],
    )

    engine._record_execution_funnel_intents("2026-01-02", [(score, intent)], market)
    report = engine._build_report()

    assert report["execution_funnel"]["entry_signal_total"] == 1
    assert report["execution_funnel"]["veto_reasons"]["below_ma20"] == 1
    route_bucket = report["execution_funnel"]["by_market_route"]["GREEN:volume_breakout"]
    assert route_bucket["entry_signal_total"] == 1
    assert route_bucket["veto_reasons"]["below_ma20"] == 1


def test_backtest_signal_validation_records_decision_and_veto_reasons():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=6.7,
        entry_signal=True,
        primary_strategy_route="volume_breakout",
        veto_triggered=True,
        hard_veto=["below_ma20"],
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.CLEAR,
        confidence=0.0,
        score=0.0,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
        notes=["一票否决", "大盘禁止新开仓"],
    )

    engine._record_signal_validation_rows("2026-01-02", [(score, intent)], market)

    row = engine._signal_records[0]
    assert row["decision_reasons"] == ["veto", "market_blocks_new_positions"]
    assert row["veto_reasons"] == ["below_ma20"]


def test_backtest_execution_funnel_labels_missing_route_as_no_entry_route():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=6.7,
        entry_signal=False,
        primary_strategy_route="",
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.WATCH,
        confidence=6.7,
        score=6.7,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
        notes=["入场信号未触发"],
    )

    engine._record_execution_funnel_intents("2026-01-02", [(score, intent)], market)
    engine._record_signal_validation_rows("2026-01-02", [(score, intent)], market)
    report = engine._build_report()

    assert report["execution_funnel"]["routes"]["no_entry_route"] == 1
    assert "unknown" not in report["execution_funnel"]["routes"]
    assert report["execution_funnel"]["by_market_route"]["GREEN:no_entry_route"]["signals"] == 1
    assert report["signal_validation"]["signals"][0]["primary_strategy_route"] == "no_entry_route"
    assert report["signal_validation"]["unknown_route_count"] == 0


def test_backtest_execution_funnel_labels_route_less_entry_signal_as_generic_route():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(
        code="300223",
        name="北京君正",
        total=3.6,
        entry_signal=True,
        primary_strategy_route="",
    )
    intent = DecisionIntent(
        code="300223",
        name="北京君正",
        action=Action.WATCH,
        confidence=3.6,
        score=3.6,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
        notes=["市场制度禁止新开仓"],
    )

    engine._record_execution_funnel_intents("2026-06-03", [(score, intent)], market)
    engine._record_signal_validation_rows("2026-06-03", [(score, intent)], market)
    report = engine._build_report()

    assert report["execution_funnel"]["routes"]["generic_entry_signal_watch"] == 1
    assert "unknown" not in report["execution_funnel"]["routes"]
    assert report["execution_funnel"]["by_market_route"]["GREEN:generic_entry_signal_watch"]["signals"] == 1
    assert report["signal_validation"]["signals"][0]["primary_strategy_route"] == "generic_entry_signal_watch"
    assert report["signal_validation"]["unknown_route_count"] == 0


def test_backtest_report_labels_production_buy_only_execution_semantics():
    engine = BacktestEngine(BacktestConfig())

    report = engine._build_report()

    assert report["execution_semantics"]["mode"] == "production_buy_only"
    assert report["execution_semantics"]["route_policy_default_actions"] == ["BUY"]
    assert report["execution_semantics"]["watch_trial_enabled"] is False


def test_backtest_report_labels_research_watch_trial_execution_semantics():
    engine = BacktestEngine(
        BacktestConfig(
            execute_watch_trial_pairs=("GREEN:relative_strength_overheat",),
            watch_loss_cooldown_days=20,
            watch_loss_cooldown_phase_buckets=("below_ma20_slope_up",),
        )
    )

    report = engine._build_report()

    assert report["execution_semantics"]["mode"] == "research_what_if"
    assert report["execution_semantics"]["watch_trial_enabled"] is True
    assert report["execution_semantics"]["watch_loss_cooldown_days"] == 20
    assert report["execution_semantics"]["watch_loss_cooldown_phases"] == ["below_ma20_slope_up"]


def test_backtest_report_can_skip_signal_alpha_summary_for_fast_execution_checks():
    engine = BacktestEngine(BacktestConfig(include_signal_alpha=False))
    engine._signal_records = [{"code": "300475", "primary_strategy_route": "relative_strength_overheat"}]

    report = engine._build_report()

    assert report["signal_alpha"] == {"skipped": True, "sample_size": 1}
    assert report["signal_validation"]["sample_size"] == 1
    assert "execution_funnel" in report


def test_backtest_report_groups_realized_trade_performance_by_market_route():
    engine = BacktestEngine(BacktestConfig(include_signal_alpha=False))
    engine._trades = [
        {
            "side": "buy",
            "source_route": "short_continuation",
            "market_signal": "GREEN",
            "trade_cost": {"total_cost": 8.0},
        },
        {
            "side": "scale_in",
            "source_route": "short_continuation",
            "market_signal": "GREEN",
            "trade_cost": {"total_cost": 3.0},
        },
        {
            "side": "sell",
            "source_route": "short_continuation",
            "market_signal": "GREEN",
            "pnl": 1200.0,
            "return_pct": 12.0,
            "trade_cost": {"total_cost": 10.0},
        },
        {
            "side": "sell",
            "source_route": "relative_strength_overheat",
            "market_signal": "YELLOW",
            "pnl": -300.0,
            "return_pct": -3.0,
            "trade_cost": {"total_cost": 7.0},
        },
    ]

    report = engine._build_report()

    perf = report["trade_performance"]["by_market_route"]
    green_short = perf["GREEN:short_continuation"]
    assert green_short["buy_trades"] == 1
    assert green_short["scale_in_trades"] == 1
    assert green_short["sell_trades"] == 1
    assert green_short["win_rate_pct"] == 100.0
    assert green_short["avg_return_pct"] == 12.0
    assert green_short["pnl"] == 1200.0
    assert green_short["trade_cost"] == 21.0

    yellow_overheat = perf["YELLOW:relative_strength_overheat"]
    assert yellow_overheat["sell_trades"] == 1
    assert yellow_overheat["win_rate_pct"] == 0.0
    assert report["trade_performance"]["by_route"]["short_continuation"]["pnl"] == 1200.0
    assert report["trade_performance"]["by_market_signal"]["GREEN"]["sell_trades"] == 1


def test_backtest_report_keeps_full_trade_log_for_persistence():
    engine = BacktestEngine(BacktestConfig(include_signal_alpha=False))
    engine._trades = [
        {
            "date": "2026-01-02",
            "code": f"60{index:04d}",
            "name": f"样本{index}",
            "side": "buy" if index % 2 == 0 else "sell",
            "price": 10.0 + index,
            "shares": 100,
            "pnl": 0,
            "return_pct": 0,
        }
        for index in range(60)
    ]

    report = engine._build_report()

    assert len(report["trade_log"]) == 60
    assert len(report["trades"]) == 50
    assert report["trades"][0]["code"] == "600010"
    assert report["trade_log"][0]["code"] == "600000"

    engine.cfg.trade_record_limit = None
    full_report = engine._build_report()
    assert len(full_report["trades"]) == 60

    engine.cfg.trade_record_limit = 0
    empty_report = engine._build_report()
    assert empty_report["trades"] == []


def test_backtest_allocation_respects_total_position_limit():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.20,
            total_max_pct=0.30,
        )
    )
    trade_date = "2026-01-02"
    engine._cash = 75000.0
    engine._sorted_dates = [trade_date]
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": [trade_date],
            "收盘": [10.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=2500,
            entry_price=10.0,
            entry_date=trade_date,
            high_water=10.0,
        )
    }

    assert engine._allocation_budget_for_position(trade_date, 0.20) == 5000.0


def test_backtest_load_data_can_skip_slow_financial_fetch(monkeypatch):
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    kline = pd.DataFrame({
        "日期": dates,
        "开盘": [10.0, 10.2, 10.3],
        "最高": [10.5, 10.6, 10.7],
        "最低": [9.8, 10.0, 10.1],
        "收盘": [10.2, 10.3, 10.4],
        "成交量": [1000000, 1100000, 1200000],
        "成交额": [10000000, 11000000, 12000000],
        "涨跌幅": [1.0, 0.98, 0.97],
        "证券名称": ["样本"] * 3,
        "名称": ["样本"] * 3,
    })

    class FakeBaoStockMarketAdapter:
        async def get_kline(self, *args, **kwargs):
            return kline.copy()

    monkeypatch.setattr(
        market_adapters,
        "BaoStockMarketAdapter",
        lambda: FakeBaoStockMarketAdapter(),
    )
    engine = BacktestEngine(BacktestConfig(load_financials=False))
    called = False

    def fail_if_called(codes, end_date):
        nonlocal called
        called = True

    monkeypatch.setattr(engine, "_load_financials", fail_if_called)

    result = engine.load_data(["600036"], "2024-01-02", "2024-01-04", "2023-10-01")

    assert result["loaded"] == 1
    assert called is False


def test_backtest_load_data_uses_market_calendar_not_stock_date_intersection(monkeypatch):
    def kline_for(dates: list[str], close_start: float) -> pd.DataFrame:
        return pd.DataFrame({
            "日期": dates,
            "开盘": [close_start + i for i, _ in enumerate(dates)],
            "最高": [close_start + i + 0.5 for i, _ in enumerate(dates)],
            "最低": [close_start + i - 0.5 for i, _ in enumerate(dates)],
            "收盘": [close_start + i for i, _ in enumerate(dates)],
            "成交量": [1000000 + i for i, _ in enumerate(dates)],
            "成交额": [10000000 + i for i, _ in enumerate(dates)],
            "涨跌幅": [0.0 for _ in dates],
            "证券名称": ["样本"] * len(dates),
            "名称": ["样本"] * len(dates),
        })

    market_dates = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
    klines = {
        "000001": kline_for(market_dates, 3000.0),
        "600036": kline_for(market_dates, 10.0),
        "300803": kline_for(["2024-01-04", "2024-01-05"], 20.0),
    }

    class FakeBaoStockMarketAdapter:
        async def get_kline(self, code, **kwargs):
            return klines[code].copy()

    monkeypatch.setattr(
        market_adapters,
        "BaoStockMarketAdapter",
        lambda: FakeBaoStockMarketAdapter(),
    )

    engine = BacktestEngine(BacktestConfig(load_financials=False))

    result = engine.load_data(["600036", "300803"], "2024-01-02", "2024-01-05", "2023-10-01")

    assert result["loaded"] == 2
    assert result["trading_days"] == 4
    assert engine._sorted_dates == market_dates
    assert engine._close_on_or_before(engine._bars["600036"], "2024-01-02") == 10.0
    assert engine._close_on_or_before(engine._bars["300803"], "2024-01-02") is None
    assert engine._close_on_or_before(engine._bars["300803"], "2024-01-04") == 20.0


def test_backtest_progress_log_writes_to_stderr(capsys):
    engine = BacktestEngine(BacktestConfig(progress_log=True))

    engine._log_progress("kline_start", code="600036", batch="1/2")

    captured = capsys.readouterr()
    assert "backtest_progress" in captured.err
    assert "kline_start" in captured.err
    assert "600036" in captured.err
    assert captured.out == ""


def test_backtest_date_ranges_cover_requested_end_date():
    ranges = _date_ranges("2023-10-03", "2025-12-31", months_per_batch=6)

    assert ranges[0][0] == "2023-10-03"
    assert ranges[-1][1] == "2025-12-31"
    assert len(ranges) == 5
    assert all(prev[1] == nxt[0] for prev, nxt in zip(ranges, ranges[1:]))


def test_financial_periods_only_include_reports_available_by_end_date():
    periods = _financial_periods("2024-01-01", "2025-12-31")

    assert (2025, 3) in periods
    assert (2025, 4) not in periods


def test_backtest_load_data_fetches_final_date_range_with_no_row_cap(monkeypatch):
    calls: list[dict] = []
    kline = pd.DataFrame({
        "日期": ["2024-01-02", "2025-12-31"],
        "开盘": [10.0, 10.2],
        "最高": [10.5, 10.6],
        "最低": [9.8, 10.0],
        "收盘": [10.2, 10.3],
        "成交量": [1000000, 1100000],
        "成交额": [10000000, 11000000],
        "涨跌幅": [1.0, 0.98],
        "证券名称": ["样本", "样本"],
        "名称": ["样本", "样本"],
    })

    class FakeBaoStockMarketAdapter:
        async def get_kline(self, code, **kwargs):
            calls.append({"code": code, **kwargs})
            return kline.copy()

    monkeypatch.setattr(
        market_adapters,
        "BaoStockMarketAdapter",
        lambda: FakeBaoStockMarketAdapter(),
    )
    engine = BacktestEngine(BacktestConfig(load_financials=False))

    result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")

    assert result["loaded"] == 1
    stock_calls = [item for item in calls if item["code"] == "600036"]
    assert stock_calls[-1]["end_date"] == "2025-12-31"
    assert all(item["count"] == 0 for item in stock_calls)


def test_backtest_load_data_uses_market_bars_cache_when_available(mysql_conn, monkeypatch):
    conn = mysql_conn
    try:
        store = MarketStore(conn)
        cached = pd.DataFrame({
            "日期": ["2023-10-04", "2024-01-02", "2025-12-31"],
            "开盘": [10.0, 10.1, 10.2],
            "最高": [10.5, 10.6, 10.7],
            "最低": [9.8, 9.9, 10.0],
            "收盘": [10.2, 10.3, 10.4],
            "成交量": [1000000, 1100000, 1200000],
            "成交额": [10000000, 11000000, 12000000],
        })
        index = cached.assign(收盘=[3000.0, 3010.0, 3020.0])
        store.save_price_bars("600036", cached, source="test", adjustflag="2")
        store.save_price_bars("000001", index, source="test", adjustflag="2")

        class FailingBaoStockMarketAdapter:
            async def get_kline(self, *args, **kwargs):
                raise AssertionError("cache hit should not call baostock")

        monkeypatch.setattr(
            market_adapters,
            "BaoStockMarketAdapter",
            lambda: FailingBaoStockMarketAdapter(),
        )
        engine = BacktestEngine(
            BacktestConfig(use_market_bars=True, load_financials=False),
            market_conn=conn,
        )

        result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")

        assert result["loaded"] == 1
        assert engine._bars["600036"]["日期"].iloc[-1] == "2025-12-31"
    finally:
        conn.close()


def test_backtest_load_data_hydrates_market_bars_after_remote_fetch(mysql_conn, monkeypatch):
    conn = mysql_conn
    try:
        kline = pd.DataFrame({
            "日期": ["2024-01-02", "2025-12-31"],
            "开盘": [10.0, 10.2],
            "最高": [10.5, 10.6],
            "最低": [9.8, 10.0],
            "收盘": [10.2, 10.3],
            "成交量": [1000000, 1100000],
            "成交额": [10000000, 11000000],
            "涨跌幅": [1.0, 0.98],
            "证券名称": ["样本", "样本"],
            "名称": ["样本", "样本"],
        })

        class FakeBaoStockMarketAdapter:
            async def get_kline(self, *args, **kwargs):
                return kline.copy()

        monkeypatch.setattr(
            market_adapters,
            "BaoStockMarketAdapter",
            lambda: FakeBaoStockMarketAdapter(),
        )
        engine = BacktestEngine(
            BacktestConfig(hydrate_market_bars=True, load_financials=False),
            market_conn=conn,
        )

        result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")
        stored = MarketStore(conn).get_price_bars(
            "600036",
            start="2024-01-01",
            end="2025-12-31",
            adjustflag="2",
        )

        assert result["loaded"] == 1
        assert not stored.empty
        assert stored["日期"].iloc[-1] == "2025-12-31"
    finally:
        conn.close()


def test_backtest_load_financials_uses_cached_snapshot(mysql_conn):
    conn = mysql_conn
    try:
        store = MarketStore(conn)
        store.save_financial_snapshot(
            "600036",
            report_year=2025,
            report_quarter=4,
            report_date="2025-12-31",
            available_date="2026-04-30",
            payload={
                "roe": 12.3,
                "roe_3y_ago": 6.1,
                "revenue_growth": 8.8,
                "operating_cash_flow": 0.2,
            },
            source="baostock",
        )
        engine = BacktestEngine(
            BacktestConfig(use_financial_cache=True),
            market_conn=conn,
        )

        engine._load_financials(["600036"], "2025-01-01", "2026-06-01")

        fin = engine._financial_for_date("600036", "2026-05-01")
        assert fin["roe"] == 12.3
        assert fin["roe_3y_ago"] == 6.1
    finally:
        conn.close()


def test_backtest_load_financials_cache_only_uses_bulk_store_without_db():
    class FakeStore:
        def __init__(self):
            self.calls = []

        def get_financial_snapshots_bulk(self, symbols, **kwargs):
            self.calls.append((list(symbols), dict(kwargs)))
            return {
                "600036": [
                    {
                        "symbol": "600036",
                        "report_year": 2025,
                        "report_quarter": 4,
                        "report_date": "2025-12-31",
                        "available_date": "2026-04-30",
                        "roe": 12.3,
                        "roe_3y_ago": 6.1,
                        "revenue_growth": 8.8,
                        "net_profit_growth": 8.8,
                        "operating_cash_flow": 0.2,
                    }
                ],
                "300750": [],
            }

    engine = BacktestEngine(
        BacktestConfig(use_financial_cache=True, hydrate_financial_cache=False),
    )
    fake_store = FakeStore()
    engine._market_store = fake_store

    engine._load_financials(["600036", "300750"], "2025-01-01", "2026-06-01")

    assert fake_store.calls == [
        (
            ["600036", "300750"],
            {"end_available": "2026-06-01", "source": "baostock"},
        )
    ]
    assert engine._financial_for_date("600036", "2026-05-01")["roe"] == 12.3
    assert engine._financial_cache["300750"] == []


def test_backtest_load_financials_uses_single_bulk_cache_query(mysql_conn):
    conn = mysql_conn
    try:
        store = MarketStore(conn)
        store.save_financial_snapshot(
            "600036",
            report_year=2025,
            report_quarter=4,
            report_date="2025-12-31",
            available_date="2026-04-30",
            payload={"roe": 12.3},
            source="baostock",
        )
        store.save_financial_snapshot(
            "300750",
            report_year=2025,
            report_quarter=4,
            report_date="2025-12-31",
            available_date="2026-04-29",
            payload={"roe": 10.1},
            source="baostock",
        )
        engine = BacktestEngine(
            BacktestConfig(use_financial_cache=True, hydrate_financial_cache=False),
            market_conn=conn,
        )
        calls = []
        original_bulk = engine._market_store.get_financial_snapshots_bulk

        def wrapped_bulk(symbols, **kwargs):
            calls.append((list(symbols), dict(kwargs)))
            return original_bulk(symbols, **kwargs)

        engine._market_store.get_financial_snapshots_bulk = wrapped_bulk

        engine._load_financials(["600036", "300750"], "2025-01-01", "2026-06-01")

        assert len(calls) == 1
        assert calls[0][0] == ["600036", "300750"]
        assert calls[0][1]["end_available"] == "2026-06-01"
        assert engine._financial_for_date("600036", "2026-05-01")["roe"] == 12.3
        assert engine._financial_for_date("300750", "2026-05-01")["roe"] == 10.1
    finally:
        conn.close()
