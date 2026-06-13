from __future__ import annotations

from astock_trading.market.models import StockSnapshot
from astock_trading.strategy.continuation_models import ContinuationFilterConfig, ContinuationFilterResult


def _limit_up_threshold(code: str) -> float:
    if code.startswith("8"):
        return 29.9
    if code.startswith(("300", "301", "688")):
        return 19.9
    return 9.9


class ContinuationQualifier:
    def __init__(self, config: ContinuationFilterConfig):
        self.config = config

    def qualify(self, snapshot: StockSnapshot) -> ContinuationFilterResult:
        if not snapshot.quote or not snapshot.technical:
            return ContinuationFilterResult(False, ["missing_quote_or_technical"])

        q = snapshot.quote
        t = snapshot.technical
        required_quote_fields = (q.open, q.high, q.low, q.close, q.amount, q.change_pct)
        if any(value is None for value in required_quote_fields):
            return ContinuationFilterResult(False, ["missing_quote_fields"])

        if q.high < q.low:
            close_near_high = 0.0
        elif q.high == q.low:
            # A one-price bar closes at its high by definition; let the dedicated
            # `limit_up_locked` filter decide whether that structure is allowed.
            close_near_high = 1.0
        else:
            close_near_high = (q.close - q.low) / (q.high - q.low)
        intraday_retrace = 0.0 if q.high <= 0 else max(0.0, (q.high - q.close) / q.high)
        reasons: list[str] = []

        if q.amount < self.config.amount_min:
            reasons.append("amount")
        if q.change_pct < self.config.change_pct_min:
            reasons.append("change_pct")
        if close_near_high < self.config.close_near_high_min:
            reasons.append("close_near_high")
        if intraday_retrace > self.config.max_intraday_retrace:
            reasons.append("intraday_retrace")
        if not (self.config.volume_ratio_min <= t.volume_ratio <= self.config.volume_ratio_max):
            reasons.append("volume_ratio")
        if self.config.require_above_ma5 and q.close < t.ma5:
            reasons.append("above_ma5")
        if self.config.exclude_limit_up_locked:
            threshold = _limit_up_threshold(q.code)
            if abs(q.change_pct) >= threshold and q.open == q.high == q.low == q.close:
                reasons.append("limit_up_locked")
        if self.config.exclude_long_upper_shadow:
            upper_shadow = q.high - max(q.open, q.close)
            body = abs(q.close - q.open)
            if upper_shadow > 0 and body > 0 and upper_shadow / body >= 1.0:
                reasons.append("long_upper_shadow")

        return ContinuationFilterResult(
            qualified=len(reasons) == 0,
            reasons=reasons,
            close_near_high=round(close_near_high, 4),
            intraday_retrace=round(intraday_retrace, 4),
        )
