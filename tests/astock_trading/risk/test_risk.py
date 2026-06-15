"""Tests for risk/rules.py and risk/sizing.py — pure functions"""

from datetime import date

from astock_trading.risk.models import RiskParams
from astock_trading.risk.rules import (
    build_threshold_snapshot,
    check_exit_signals,
    check_portfolio_risk,
    get_risk_params,
)
from astock_trading.risk.sizing import calc_position_size, volatility_adjusted_stop_loss
from astock_trading.strategy.models import Style


def test_stop_loss_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=45.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
    )
    stop_signals = [s for s in signals if s.signal_type == "stop_loss"]
    assert len(stop_signals) >= 1
    assert stop_signals[0].urgency == "immediate"


def test_stop_loss_not_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=48.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
    )
    stop_signals = [s for s in signals if s.signal_type == "stop_loss"]
    assert len(stop_signals) == 0


def test_threshold_snapshot_keeps_non_triggered_risk_lines():
    snapshot = build_threshold_snapshot(
        code="002138",
        avg_cost=10.0,
        current_price=11.0,
        entry_date=date(2026, 4, 1),
        today=date(2026, 4, 6),
        highest_since_entry=12.0,
        entry_day_low=9.8,
        params=RiskParams(
            style=Style.MOMENTUM,
            stop_loss=0.08,
            trailing_stop=0.10,
            time_stop_days=15,
            exit_ma=20,
        ),
        ma20=10.5,
        ma60=9.5,
        signals=[],
    )

    assert snapshot["code"] == "002138"
    assert snapshot["holding_days"] == 5
    assert snapshot["triggered_signal_types"] == []
    assert snapshot["thresholds"]["stop_loss"] == {
        "enabled": True,
        "trigger_price": 9.2,
        "distance_pct": 0.1957,
        "triggered": False,
    }
    assert snapshot["thresholds"]["trailing_stop"] == {
        "enabled": True,
        "trigger_price": 10.8,
        "distance_pct": 0.0185,
        "triggered": False,
    }
    assert snapshot["thresholds"]["ma_exit"] == {
        "enabled": True,
        "ma": 20,
        "trigger_price": 10.5,
        "distance_pct": 0.0476,
        "triggered": False,
    }


def test_trailing_stop_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=49.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=56.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, trailing_stop=0.10),
    )
    trail = [s for s in signals if s.signal_type == "trailing_stop"]
    assert len(trail) == 1
    assert trail[0].trigger_price == round(56.0 * 0.9, 2)


def test_time_stop_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=50.5,
        entry_date=date(2026, 3, 1), today=date(2026, 4, 10),
        highest_since_entry=51.0, entry_day_low=49.0,
        params=RiskParams(style=Style.MOMENTUM, time_stop_days=15),
    )
    time_signals = [s for s in signals if s.signal_type == "time_stop"]
    assert len(time_signals) == 1
    assert time_signals[0].urgency == "advisory"


def test_ma_exit_triggered():
    signals = check_exit_signals(
        code="002138", avg_cost=50.0, current_price=13.5,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=52.0, entry_day_low=49.0,
        params=RiskParams(style=Style.SLOW_BULL, exit_ma=20),
        ma20=14.0,
    )
    ma_signals = [s for s in signals if s.signal_type == "ma_exit"]
    assert len(ma_signals) == 1


def test_get_risk_params_slow_bull():
    p = get_risk_params(Style.SLOW_BULL, {"slow_bull": {"stop_loss": 0.08, "time_stop_days": 30}})
    assert p.stop_loss == 0.08
    assert p.time_stop_days == 30
    assert p.trailing_stop is None


def test_get_risk_params_momentum():
    p = get_risk_params(Style.MOMENTUM, {"momentum": {"trailing_stop": 0.10}})
    assert p.trailing_stop == 0.10


def test_portfolio_risk_daily_loss():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.04,
        consecutive_loss_days=1,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.30,
        limits={"daily_loss_limit_pct": 0.03},
    )
    assert any(b.rule == "daily_loss_limit" for b in breaches)


def test_portfolio_risk_no_breach():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.01,
        consecutive_loss_days=0,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.30,
        limits={"daily_loss_limit_pct": 0.03, "consecutive_loss_days_limit": 2},
    )
    assert len(breaches) == 0


# ── sizing tests ──

def test_position_size_basic():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.2,
        price=15.0, market_multiplier=1.0,
    )
    assert ps.shares > 0
    assert ps.shares % 100 == 0
    assert ps.pct <= 0.20
    assert ps.amount > 0


def test_position_size_red_market():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.0,
        price=15.0, market_multiplier=0.0,
    )
    assert ps.shares == 0
    assert ps.amount == 0


def test_position_size_respects_total_limit():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.55,
        price=15.0, market_multiplier=1.0,
        total_max_pct=0.60,
    )
    assert ps.pct <= 0.05 + 0.001  # only 5% remaining


def test_position_size_yellow_market():
    ps = calc_position_size(
        total_capital=450000, current_exposure_pct=0.0,
        price=15.0, market_multiplier=0.5,
    )
    assert ps.pct <= 0.10 + 0.001  # 20% * 0.5 = 10%


def test_position_size_shrinks_high_volatility_candidates():
    ps = calc_position_size(
        total_capital=500000,
        current_exposure_pct=0.0,
        price=25.0,
        market_multiplier=1.0,
        single_max_pct=0.20,
        daily_atr_pct=0.05,
        target_vol_pct=0.02,
    )

    assert ps.pct <= 0.08 + 0.001
    assert ps.shares % 100 == 0


def test_volatility_adjusted_stop_loss_uses_two_atr_with_floor_and_cap():
    assert volatility_adjusted_stop_loss(base_stop_loss=0.08, daily_atr_pct=0.015) == 0.05
    assert volatility_adjusted_stop_loss(base_stop_loss=0.08, daily_atr_pct=0.04) == 0.08
    assert volatility_adjusted_stop_loss(base_stop_loss=0.08, daily_atr_pct=0.20) == 0.12


# ── sector concentration tests (#15) ──

def test_portfolio_risk_sector_concentration():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.01,
        consecutive_loss_days=0,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.45,
        limits={"max_sector_exposure_warn_pct": 0.40},
    )
    sector_breaches = [b for b in breaches if b.rule == "sector_concentration"]
    assert len(sector_breaches) == 1
    assert sector_breaches[0].current_value == 0.45
    assert sector_breaches[0].limit_value == 0.40


def test_portfolio_risk_sector_ok():
    breaches = check_portfolio_risk(
        daily_pnl_pct=-0.01,
        consecutive_loss_days=0,
        max_single_exposure_pct=0.15,
        max_sector_exposure_pct=0.30,
        limits={"max_sector_exposure_warn_pct": 0.40},
    )
    sector_breaches = [b for b in breaches if b.rule == "sector_concentration"]
    assert len(sector_breaches) == 0


# ── MA exit tests (verifying #2 fix works with real MA values) ──

def test_ma20_exit_with_real_ma():
    """MA20 exit should trigger when ma20 > 0 is provided."""
    signals = check_exit_signals(
        code="600066", avg_cost=36.0, current_price=34.5,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=38.0, entry_day_low=35.0,
        params=RiskParams(style=Style.SLOW_BULL, stop_loss=0.08, exit_ma=20),
        ma20=35.0,
    )
    ma_signals = [s for s in signals if s.signal_type == "ma_exit"]
    assert len(ma_signals) == 1
    assert ma_signals[0].urgency == "end_of_day"


def test_ma60_absolute_stop_with_real_ma():
    """MA60 absolute stop should trigger when ma60 > 0 is provided."""
    signals = check_exit_signals(
        code="600066", avg_cost=36.0, current_price=32.0,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=38.0, entry_day_low=35.0,
        params=RiskParams(style=Style.SLOW_BULL, stop_loss=0.08, absolute_stop_ma=60),
        ma60=34.0,
    )
    stop_signals = [s for s in signals if s.signal_type == "stop_loss" and "MA60" in s.description]
    assert len(stop_signals) == 1
    assert stop_signals[0].urgency == "immediate"


def test_ma_exit_not_triggered_without_ma_data():
    """MA exit should NOT trigger when ma20=0 (no data)."""
    signals = check_exit_signals(
        code="600066", avg_cost=36.0, current_price=34.5,
        entry_date=date(2026, 4, 1), today=date(2026, 4, 10),
        highest_since_entry=38.0, entry_day_low=35.0,
        params=RiskParams(style=Style.SLOW_BULL, stop_loss=0.08, exit_ma=20),
        ma20=0,
    )
    ma_signals = [s for s in signals if s.signal_type == "ma_exit"]
    assert len(ma_signals) == 0


# ── get_risk_params with config (#11) ──

def test_get_risk_params_reads_config():
    """get_risk_params should use config values when provided."""
    cfg = {
        "slow_bull": {"stop_loss": 0.06, "time_stop_days": 25, "exit_ma": 20},
        "momentum": {"trailing_stop": 0.12, "time_stop_days": 10},
    }
    p_sb = get_risk_params(Style.SLOW_BULL, cfg)
    assert p_sb.stop_loss == 0.06
    assert p_sb.time_stop_days == 25

    p_mm = get_risk_params(Style.MOMENTUM, cfg)
    assert p_mm.trailing_stop == 0.12
    assert p_mm.time_stop_days == 10


def test_get_risk_params_defaults_without_config():
    """get_risk_params should use defaults when no config."""
    p = get_risk_params(Style.SLOW_BULL)
    assert p.stop_loss == 0.08
    assert p.time_stop_days == 30
