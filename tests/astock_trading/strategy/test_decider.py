"""Tests for strategy/decider.py — pure function decisions"""

import pytest

from astock_trading.strategy.decider import Decider, build_decider_from_config
from astock_trading.strategy.models import (
    Action,
    DataQuality,
    MarketSignal,
    MarketState,
    ScoreResult,
    StrategyRouteEvidence,
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
        strategy_routes=kw.get("strategy_routes", []),
        primary_strategy_route=kw.get("primary_strategy_route"),
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

    assert d.action == Action.TRIAL_BUY
    assert d.market_multiplier == 0.0
    assert "试买意向" in " ".join(d.notes)


def test_regime_overlay_can_disable_red_clear_trial_buy():
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        trial_buy_threshold=6.0,
        market_regime_overlays={
            "RED": {"allow_trial_buy": False, "buy_threshold": 7.0},
            "CLEAR": {"allow_trial_buy": False, "buy_threshold": 7.0},
        },
    )
    score = _make_score(8.0, entry_signal=True)

    d = decider.decide(score, MarketState(signal=MarketSignal.RED, multiplier=0.0))

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0
    assert "制度阻断观察" in " ".join(d.notes)


def test_green_regime_overlay_raises_buy_line_without_switching_profile():
    decider = build_decider_from_config(
        {
            "scoring": {
                "thresholds": {"buy": 6.0, "watch": 5.0, "reject": 4.0},
                "decision_gates": {
                    "require_entry_signal_for_buy": True,
                    "trial_buy_entry_signal_threshold": 5.5,
                    "min_data_quality_for_buy": "ok",
                    "max_missing_fields_for_buy": 0,
                },
                "market_regime_overlays": {
                    "GREEN": {"buy_threshold": 6.5}
                },
            },
            "risk": {"position": {"single_max": 0.2, "total_max": 0.6}},
        }
    )
    score = _make_score(6.2, entry_signal=True)

    d = decider.decide(score, MarketState(signal=MarketSignal.GREEN, multiplier=1.0))

    assert d.action == Action.TRIAL_BUY
    assert d.position_pct == 0.0
    assert d.score == 6.2
    assert decider.buy_threshold == 6.0
    assert "市场制度 GREEN 买入线 6.5" in " ".join(d.notes)


def test_yellow_regime_overlay_disables_trial_buy_and_raises_buy_line():
    decider = build_decider_from_config(
        {
            "scoring": {
                "thresholds": {"buy": 6.0, "watch": 5.0, "reject": 4.0},
                "decision_gates": {
                    "require_entry_signal_for_buy": True,
                    "trial_buy_entry_signal_threshold": 5.5,
                    "min_data_quality_for_buy": "ok",
                    "max_missing_fields_for_buy": 0,
                },
                "market_regime_overlays": {
                    "YELLOW": {"buy_threshold": 6.5, "allow_trial_buy": False}
                },
            },
            "risk": {"position": {"single_max": 0.2, "total_max": 0.6}},
        }
    )
    score = _make_score(6.2, entry_signal=True)

    d = decider.decide(score, MarketState(signal=MarketSignal.YELLOW, multiplier=0.5))

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0
    assert "市场制度 YELLOW 买入线 6.5" in " ".join(d.notes)


def test_regime_overlay_blocks_disabled_trial_route_only():
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        trial_buy_threshold=6.0,
        market_regime_overlays={
            "RED": {
                "allow_trial_buy": True,
                "disabled_trial_routes": ["shrink_pullback"],
            }
        },
    )
    score = _make_score(
        6.4,
        strategy_routes=[
            StrategyRouteEvidence(
                route="shrink_pullback",
                display_name="缩量回踩",
                family="trend_swing",
                confidence=0.84,
                entry_signal=True,
            )
        ],
        entry_signal=True,
    )

    d = decider.decide(score, MarketState(signal=MarketSignal.RED, multiplier=0.0))

    assert d.action == Action.WATCH
    assert "市场制度禁用试买路线" in " ".join(d.notes)


def test_regime_overlay_can_allow_only_specific_trial_routes():
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        trial_buy_threshold=6.0,
        market_regime_overlays={
            "RED": {
                "allow_trial_buy": True,
                "enabled_trial_routes": ["pullback_to_ma20"],
            }
        },
    )
    pullback = _make_score(
        6.4,
        entry_signal=True,
        primary_strategy_route="pullback_to_ma20",
        strategy_routes=[
            StrategyRouteEvidence(
                route="pullback_to_ma20",
                display_name="均线回踩转强",
                family="trend_swing",
                confidence=0.86,
                entry_signal=True,
            )
        ],
    )
    short = _make_score(
        6.4,
        entry_signal=True,
        primary_strategy_route="short_continuation",
        strategy_routes=[
            StrategyRouteEvidence(
                route="short_continuation",
                display_name="短续接力",
                family="short_continuation",
                confidence=0.9,
                entry_signal=True,
            )
        ],
    )

    allowed = decider.decide(pullback, MarketState(signal=MarketSignal.RED, multiplier=0.0))
    blocked = decider.decide(short, MarketState(signal=MarketSignal.RED, multiplier=0.0))

    assert allowed.action == Action.TRIAL_BUY
    assert blocked.action == Action.WATCH
    assert "只允许试买路线" in " ".join(blocked.notes)


def test_red_market_low_score_stays_watch(decider):
    score = _make_score(5.4)
    market = MarketState(signal=MarketSignal.RED, multiplier=0.0)
    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0


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
    """A high score without a configured entry signal becomes a non-executable trial buy."""
    decider = Decider(
        buy_threshold=5.5,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
    )
    score = _make_score(6.8, entry_signal=False)
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.TRIAL_BUY
    assert d.position_pct == 0.0
    assert "入场信号未触发" in " ".join(d.notes)


def test_entry_signal_near_buy_line_becomes_trial_buy():
    """入场信号成立但总分略低于买入线时，应给不可执行的试买意向。"""
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
        min_data_quality_for_buy="ok",
        max_missing_fields_for_buy=0,
        trial_buy_entry_signal_threshold=5.5,
    )
    score = _make_score(
        5.6,
        entry_signal=True,
        data_quality=DataQuality.OK,
        data_missing_fields=[],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.TRIAL_BUY
    assert d.position_pct == 0.0
    assert "入场信号已触发" in " ".join(d.notes)
    assert "正式买入线" in " ".join(d.notes)


def test_low_score_entry_signal_stays_watch():
    """入场信号不能把低于试买线的标的抬成试买意向。"""
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
        min_data_quality_for_buy="ok",
        max_missing_fields_for_buy=0,
        trial_buy_entry_signal_threshold=5.5,
    )
    score = _make_score(
        5.2,
        entry_signal=True,
        data_quality=DataQuality.OK,
        data_missing_fields=[],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0


def test_build_decider_from_config_reads_trial_buy_thresholds():
    """配置可单独调试买推荐层，正式买入线不被放宽。"""
    decider = build_decider_from_config(
        {
            "scoring": {
                "thresholds": {"buy": 6.0, "watch": 5.0, "reject": 4.0},
                "decision_gates": {
                    "require_entry_signal_for_buy": True,
                    "trial_buy_threshold": 6.0,
                    "trial_buy_entry_signal_threshold": 5.5,
                    "min_data_quality_for_buy": "ok",
                    "max_missing_fields_for_buy": 0,
                },
            },
            "risk": {"position": {"single_max": 0.2, "total_max": 0.6}},
        }
    )

    assert decider.buy_threshold == 6.0
    assert decider.trial_buy_threshold == 6.0
    assert decider.trial_buy_entry_signal_threshold == 5.5


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


def test_watch_route_near_buy_line_becomes_trial_buy():
    """软路线成立且接近试买线时，可降级为不可自动承接的试买意向。"""
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
        min_data_quality_for_buy="ok",
        max_missing_fields_for_buy=0,
        trial_buy_entry_signal_threshold=5.5,
    )
    score = _make_score(
        5.6,
        entry_signal=False,
        data_quality=DataQuality.OK,
        data_missing_fields=[],
        strategy_routes=[
            StrategyRouteEvidence(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                confidence=0.62,
                entry_signal=False,
                status="watch",
                route_score=0.75,
                matched_conditions=["above_ma20", "ma20_slope", "momentum_5d"],
                missing_conditions=["volume_ratio"],
            )
        ],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.TRIAL_BUY
    assert d.position_pct == 0.0
    assert "观察路线" in " ".join(d.notes)
    assert "试买意向不形成可自动承接买入意向" in " ".join(d.notes)


def test_low_score_watch_route_stays_watch():
    """观察路线不能把低于试买线的标的抬成试买意向。"""
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
        min_data_quality_for_buy="ok",
        max_missing_fields_for_buy=0,
        trial_buy_entry_signal_threshold=5.5,
    )
    score = _make_score(
        5.2,
        entry_signal=False,
        data_quality=DataQuality.OK,
        data_missing_fields=[],
        strategy_routes=[
            StrategyRouteEvidence(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                confidence=0.62,
                entry_signal=False,
                status="watch",
                route_score=0.75,
            )
        ],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0


def test_watch_route_with_error_data_quality_stays_watch():
    """观察路线仍受数据质量门槛约束。"""
    decider = Decider(
        buy_threshold=6.0,
        watch_threshold=5.0,
        require_entry_signal_for_buy=True,
        min_data_quality_for_buy="ok",
        max_missing_fields_for_buy=0,
        trial_buy_entry_signal_threshold=5.5,
    )
    score = _make_score(
        5.8,
        entry_signal=False,
        data_quality=DataQuality.ERROR,
        data_missing_fields=[],
        strategy_routes=[
            StrategyRouteEvidence(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                confidence=0.62,
                entry_signal=False,
                status="watch",
                route_score=0.75,
            )
        ],
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    d = decider.decide(score, market)

    assert d.action == Action.WATCH
    assert d.position_pct == 0.0
