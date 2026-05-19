"""Named domain event contracts and a small publisher wrapper."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SCORE_CALCULATED = "score.calculated"
DECISION_SUGGESTED = "decision.suggested"
STRATEGY_CALIBRATION_PROPOSED = "strategy.calibration.proposed"
STRATEGY_PROFILE_COMPARISON_PROPOSED = "strategy.profile_comparison.proposed"
STRATEGY_CAPITAL_ALLOCATION_PROPOSED = "strategy.capital_allocation.proposed"
STRATEGY_HEALTH_REPORT_PROPOSED = "strategy.health_report.proposed"
RISK_ADAPTIVE_SUGGESTION_PROPOSED = "risk.adaptive_suggestion.proposed"
MANUAL_TRADE_REQUESTED = "manual_trade.requested"
TRADE_HYPOTHESIS_RECORDED = "trade.hypothesis.recorded"
TRADE_OUTCOME_RECORDED = "trade.outcome.recorded"
TRADE_REVIEW_RECORDED = "trade.review.recorded"
RULE_DEVIATION_RECORDED = "rule_deviation.recorded"
EVIDENCE_BACKFILLED = "evidence.backfilled"
CANDIDATE_ADDED = "candidate.added"
AUTO_TRADE_DIAGNOSTIC = "auto_trade.diagnostic"
AUTO_TRADE_EXECUTED = "auto_trade.executed"
AUTO_TRADE_SUMMARY = "auto_trade.summary"


@dataclass(frozen=True)
class DomainEvent:
    """Append-only event payload with an explicit event contract name."""

    stream: str
    stream_type: str
    event_type: str
    payload: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


class DomainEventPublisher:
    """Thin typed facade over EventStore.append()."""

    def __init__(self, event_store: Any):
        self._event_store = event_store

    def publish(self, event: DomainEvent) -> str:
        return self._event_store.append(
            stream=event.stream,
            stream_type=event.stream_type,
            event_type=event.event_type,
            payload=event.payload,
            metadata=event.metadata,
        )
