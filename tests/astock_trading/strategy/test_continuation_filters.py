from astock_trading.market.models import StockQuote, StockSnapshot, TechnicalIndicators
from astock_trading.strategy.continuation_filters import ContinuationQualifier
from astock_trading.strategy.continuation_models import ContinuationFilterConfig


def _make_snapshot(**overrides):
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.0,
        open=14.5,
        high=15.2,
        low=14.4,
        close=15.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )
    technical = TechnicalIndicators(
        ma5=14.6,
        ma10=14.3,
        ma20=13.9,
        ma60=13.0,
        above_ma20=True,
        volume_ratio=1.8,
        rsi=62.0,
        golden_cross=False,
        deviation_rate=2.8,
        change_pct=3.5,
    )
    payload = dict(code="002138", name="双环传动", quote=quote, technical=technical)
    payload.update(overrides)
    return StockSnapshot(**payload)


def test_qualifier_accepts_clean_short_continuation_candidate():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    result = qualifier.qualify(_make_snapshot())

    assert result.qualified is True
    assert result.reasons == []


def test_qualifier_rejects_deep_intraday_retrace():
    qualifier = ContinuationQualifier(ContinuationFilterConfig(max_intraday_retrace=0.03))
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.03,
        open=14.5,
        high=15.5,
        low=13.6,
        close=15.03,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )

    result = qualifier.qualify(_make_snapshot(quote=quote))

    assert result.qualified is False
    assert "intraday_retrace" in result.reasons


def test_qualifier_rejects_when_quote_or_technical_is_missing():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())

    missing_quote = qualifier.qualify(_make_snapshot(quote=None))
    missing_technical = qualifier.qualify(_make_snapshot(technical=None))

    assert missing_quote.qualified is False
    assert missing_quote.reasons == ["missing_quote_or_technical"]
    assert missing_technical.qualified is False
    assert missing_technical.reasons == ["missing_quote_or_technical"]


def test_qualifier_rejects_when_quote_range_fields_are_missing():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=15.0,
        open=14.5,
        high=None,
        low=14.4,
        close=15.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )

    result = qualifier.qualify(_make_snapshot(quote=quote))

    assert result.qualified is False
    assert result.reasons == ["missing_quote_fields"]


def test_qualifier_rejects_long_upper_shadow_when_enabled():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=14.8,
        open=14.7,
        high=15.3,
        low=14.6,
        close=14.8,
        volume=5_000_000,
        amount=3e8,
        change_pct=3.5,
    )

    result = qualifier.qualify(_make_snapshot(quote=quote))

    assert result.qualified is False
    assert "long_upper_shadow" in result.reasons


def test_qualifier_rejects_limit_up_locked_for_chinext_threshold():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    quote = StockQuote(
        code="300750",
        name="宁德时代",
        price=24.0,
        open=24.0,
        high=24.0,
        low=24.0,
        close=24.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=20.0,
    )

    result = qualifier.qualify(_make_snapshot(code="300750", name="宁德时代", quote=quote))

    assert result.qualified is False
    assert "limit_up_locked" in result.reasons


def test_qualifier_does_not_reject_beijing_board_below_30pct_lock():
    qualifier = ContinuationQualifier(ContinuationFilterConfig())
    quote = StockQuote(
        code="830001",
        name="北交示例",
        price=24.0,
        open=24.0,
        high=24.0,
        low=24.0,
        close=24.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=20.0,
    )

    result = qualifier.qualify(_make_snapshot(code="830001", name="北交示例", quote=quote))

    assert result.qualified is True
    assert "limit_up_locked" not in result.reasons


def test_qualifier_can_allow_locked_bar_when_flag_disabled():
    qualifier = ContinuationQualifier(ContinuationFilterConfig(exclude_limit_up_locked=False))
    quote = StockQuote(
        code="300750",
        name="宁德时代",
        price=24.0,
        open=24.0,
        high=24.0,
        low=24.0,
        close=24.0,
        volume=5_000_000,
        amount=3e8,
        change_pct=20.0,
    )

    result = qualifier.qualify(_make_snapshot(code="300750", name="宁德时代", quote=quote))

    assert result.qualified is True
    assert "limit_up_locked" not in result.reasons
