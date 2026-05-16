"""Tests for strategy/decider.py — pure function decisions"""

import pytest

from astock_trading.strategy.decider import Decider
from astock_trading.strategy.models import (
    Action,
    DataQuality,
    MarketSignal,
    MarketState,
    ScoreResult,
    Style,
)


def _make_score(total: float = 7.0, veto: bool = False, **kw) -> ScoreResult:
    return ScoreResult(
        code=kw.get("code", "002138"),
        name=kw.get("name", "双环传动"),
        total=0.0 if veto else total,
        veto_triggered=veto,
        hard_veto=["below_ma20"] if veto else [],
        style=Style.MOMENTUM,
        entry_signal=kw.get("entry_signal", False),
        data_quality=kw.get("data_quality", DataQuality.OK),
        data_missing_fields=kw.get("data_missing_fields", []),
    )


@pytest.fixture
def decider():
    return Decider(buy_threshold=6.5, watch_threshold=5.0, weekly_max=2)


def test_buy_decision(decider):
    score = _make_score(7.5)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market)

    assert d.action == Action.BUY
    assert d.position_pct > 0
    assert d.market_multiplier == 1.0


def test_watch_decision(decider):
    score = _make_score(5.5)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market)

    assert d.action == Action.WATCH


def test_clear_low_score(decider):
    score = _make_score(3.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market)

    assert d.action == Action.CLEAR


def test_veto_blocks_buy(decider):
    score = _make_score(veto=True)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market)

    assert d.action == Action.CLEAR
    assert "below_ma20" in d.veto_reasons


def test_red_market_blocks_buy(decider):
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.RED, multiplier=0.0)
    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert d.market_multiplier == 0.0


def test_yellow_market_reduces_position(decider):
    score = _make_score(7.5)
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)
    d = decider.decide(score, market)

    assert d.action == Action.BUY
    assert d.position_pct <= 0.10 + 0.001  # 20% * 0.5


def test_weekly_limit_blocks_buy(decider):
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market, weekly_buy_count=2)

    # weekly limit reached, should not BUY
    assert d.action != Action.BUY or "本周已买" in " ".join(d.notes)


def test_batch_decide(decider):
    scores = [_make_score(7.5, code="001"), _make_score(5.0, code="002")]
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    decisions = decider.decide_batch(scores, market)

    assert len(decisions) == 2
    assert decisions[0].code == "001"


# ── exposure limit tests (#3) ──

def test_exposure_limit_caps_position(decider):
    """When near total_max, position_pct should be capped to remaining."""
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market, current_exposure_pct=0.55)

    assert d.action == Action.BUY
    # total_max=0.60, current=0.55, remaining=0.05
    assert d.position_pct <= 0.05 + 0.001


def test_full_exposure_blocks_buy(decider):
    """When at total_max, Decider should not emit a zero-size BUY."""
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market, current_exposure_pct=0.60)

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0
    assert "仓位空间不足" in " ".join(d.notes)


def test_weekly_limit_exact_boundary(decider):
    """weekly_buy_count == weekly_max should block BUY."""
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market, weekly_buy_count=2)

    assert d.action == Action.WATCH  # blocked by weekly limit


def test_weekly_limit_under(decider):
    """weekly_buy_count < weekly_max should allow BUY."""
    score = _make_score(8.0)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    d = decider.decide(score, market, weekly_buy_count=1)

    assert d.action == Action.BUY


def test_entry_signal_gate_blocks_high_score_buy():
    """A high score without a configured entry signal remains WATCH."""
    decider = Decider(
        buy_threshold=5.5,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
    )
    score = _make_score(6.8, entry_signal=False)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert "入场信号未触发" in " ".join(d.notes)


def test_data_quality_gate_blocks_buy_when_too_many_fields_missing():
    """High-score candidates with incomplete critical inputs should not be bought."""
    decider = Decider(
        buy_threshold=5.5,
        watch_threshold=5.0,
        max_missing_fields_for_buy=1,
    )
    score = _make_score(
        6.8,
        entry_signal=True,
        data_quality=DataQuality.DEGRADED,
        data_missing_fields=["ROE", "营收", "现金流"],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert "关键数据缺失过多" in " ".join(d.notes)


def test_min_data_quality_gate_blocks_buy():
    """A strict OK-only profile can block degraded data from buying."""
    decider = Decider(
        buy_threshold=5.5,
        watch_threshold=5.0,
        min_data_quality_for_buy="ok",
    )
    score = _make_score(6.8, entry_signal=True, data_quality=DataQuality.DEGRADED)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert "数据质量" in " ".join(d.notes)
