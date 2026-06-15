"""
strategy/service.py — 策略服务层

编排评分 + 决策，结果写入 event_log。
这是 strategy context 唯一允许做 IO（写事件）的地方。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from astock_trading.market.models import StockSnapshot
from astock_trading.platform.domain_events import (
    DECISION_SUGGESTED,
    DomainEvent,
    DomainEventPublisher,
    MANUAL_TRADE_REQUESTED,
    SCORE_CALCULATED,
)
from astock_trading.platform.events import EventStore
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.models import Action, DataQuality, DecisionIntent, MarketState, ScoreResult
from astock_trading.strategy.cooldown import false_breakout_cooldown
from astock_trading.strategy.scorer import Scorer

_logger = logging.getLogger(__name__)


class StrategyService:
    """编排评分 + 决策，结果追加到 event_log。"""

    def __init__(
        self,
        scorer: Scorer,
        decider: Decider,
        event_store: EventStore,
        manual_trade_notifier: Callable[[dict[str, Any]], None] | None = None,
        false_breakout_cfg: dict[str, Any] | None = None,
    ):
        self._scorer = scorer
        self._decider = decider
        self._event_store = event_store
        self._publisher = DomainEventPublisher(event_store)
        self._manual_trade_notifier = manual_trade_notifier
        self._false_breakout_cfg = false_breakout_cfg or {}

    def evaluate(
        self,
        snapshots: list[StockSnapshot],
        market_state: MarketState,
        run_id: str,
        config_version: str,
        current_exposure_pct: float = 0.0,
        weekly_buy_count: int = 0,
    ) -> list[DecisionIntent]:
        """
        批量评分 + 决策。

        1. 对每个 snapshot 评分 → ScoreResult
        2. 每个 ScoreResult 追加 score.calculated 事件
        3. 对每个 ScoreResult 决策 → DecisionIntent
        4. 每个 DecisionIntent 追加 decision.suggested 事件

        Returns:
            按评分降序排列的 DecisionIntent 列表
        """
        results = self._scorer.score_batch(snapshots)
        metadata = {"run_id": run_id, "config_version": config_version}

        decisions: list[DecisionIntent] = []

        for score_result in results:
            score_result = self._apply_false_breakout_cooldown(score_result)
            snapshot = next((s for s in snapshots if s.code == score_result.code), None)
            score_payload = self._score_payload_with_reference(score_result)
            score_evidence = _score_evidence_payload(score_payload)
            if snapshot and snapshot.observation_id:
                score_payload["source_observation_id"] = snapshot.observation_id

            # 追加评分事件
            score_event_id = self._publisher.publish(DomainEvent(
                stream=f"strategy:{score_result.code}",
                stream_type="strategy",
                event_type=SCORE_CALCULATED,
                payload=score_payload,
                metadata=metadata,
            ))

            # 决策
            decision = self._decider.decide(
                score_result,
                market_state,
                current_exposure_pct=current_exposure_pct,
                weekly_buy_count=weekly_buy_count,
            )
            decision = self._decision_with_false_breakout_cooldown(decision, score_result)
            decisions.append(decision)
            buy_funnel = self._decider.build_buy_funnel(
                score_result,
                market_state,
                decision=decision,
                current_exposure_pct=current_exposure_pct,
                weekly_buy_count=weekly_buy_count,
            )

            # 追加决策事件
            decision_event_id = self._publisher.publish(DomainEvent(
                stream=f"strategy:{decision.code}",
                stream_type="strategy",
                event_type=DECISION_SUGGESTED,
                payload={
                    "code": decision.code,
                    "name": decision.name,
                    "action": decision.action.value,
                    "confidence": decision.confidence,
                    "score": decision.score,
                    "position_pct": decision.position_pct,
                    "market_signal": decision.market_signal.value,
                    "market_multiplier": decision.market_multiplier,
                    "veto_reasons": decision.veto_reasons,
                    "notes": decision.notes,
                    "source_score_event_id": score_event_id,
                    **score_evidence,
                    "decision_inputs": {
                        "current_exposure_pct": current_exposure_pct,
                        "weekly_buy_count": weekly_buy_count,
                    },
                    "buy_funnel": buy_funnel,
                    "market_state": _market_state_payload(market_state),
                    "decision_rules": _decision_rules_payload(self._decider),
                },
                metadata=metadata,
            ))

            if decision.action == Action.BUY:
                quote = snapshot.quote if snapshot else None
                manual_payload = {
                    "status": "pending",
                    "side": "buy",
                    "code": decision.code,
                    "name": decision.name,
                    "score": decision.score,
                    "confidence": decision.confidence,
                    "position_pct": decision.position_pct,
                    "suggested_price": quote.close if quote else 0,
                    "market_signal": decision.market_signal.value,
                    "market_multiplier": decision.market_multiplier,
                    "source_event_id": decision_event_id,
                    "source_score_event_id": score_event_id,
                    "buy_funnel": buy_funnel,
                    **score_evidence,
                }
                manual_metadata = {**metadata, "account": "main", "execution": "manual"}
                manual_event_id = self._publisher.publish(DomainEvent(
                    stream=f"manual_trade:{decision.code}",
                    stream_type="manual_trade",
                    event_type=MANUAL_TRADE_REQUESTED,
                    payload=manual_payload,
                    metadata=manual_metadata,
                ))
                self._notify_manual_trade_requested(
                    event_id=manual_event_id,
                    manual_trade=manual_payload,
                    metadata=manual_metadata,
                    score_result=score_result,
                    decision=decision,
                    snapshot=snapshot,
                    market_state=market_state,
                )

        return decisions

    def _notify_manual_trade_requested(
        self,
        *,
        event_id: str,
        manual_trade: dict[str, Any],
        metadata: dict[str, Any],
        score_result: ScoreResult,
        decision: DecisionIntent,
        snapshot: StockSnapshot | None,
        market_state: MarketState,
    ) -> None:
        if self._manual_trade_notifier is None:
            return
        try:
            self._manual_trade_notifier({
                "event_id": event_id,
                "event_type": MANUAL_TRADE_REQUESTED,
                "manual_trade": manual_trade,
                "metadata": metadata,
                "score_result": score_result,
                "decision": decision,
                "snapshot": snapshot,
                "market_state": market_state,
            })
        except Exception as exc:
            _logger.warning("[strategy] 人工确认 Discord 通知失败: %s", exc)

    def score_single(
        self,
        snapshot: StockSnapshot,
        run_id: str,
        config_version: str,
    ) -> ScoreResult:
        """单股评分，结果追加到 event_log。"""
        result = self._apply_false_breakout_cooldown(self._scorer.score(snapshot))

        self._publisher.publish(DomainEvent(
            stream=f"strategy:{result.code}",
            stream_type="strategy",
            event_type=SCORE_CALCULATED,
            payload=self._score_payload_with_reference(result),
            metadata={"run_id": run_id, "config_version": config_version},
        ))

        return result

    def _apply_false_breakout_cooldown(self, score_result: ScoreResult) -> ScoreResult:
        cooldown = false_breakout_cooldown(
            self._event_store,
            score_result.code,
            self._false_breakout_cfg,
        )
        if not cooldown.get("active") or not score_result.entry_signal:
            return replace(score_result, false_breakout_cooldown=cooldown)

        routes = [
            replace(
                route,
                entry_signal=False,
                status="watch",
                notes=list(route.notes) + ["false_breakout_cooldown"],
            )
            if route.entry_signal
            else route
            for route in score_result.strategy_routes
        ]
        warnings = list(score_result.warning_signals)
        if "false_breakout_cooldown" not in warnings:
            warnings.append("false_breakout_cooldown")
        return replace(
            score_result,
            entry_signal=False,
            warning_signals=warnings,
            strategy_routes=routes,
            false_breakout_cooldown=cooldown,
        )

    @staticmethod
    def _decision_with_false_breakout_cooldown(
        decision: DecisionIntent,
        score_result: ScoreResult,
    ) -> DecisionIntent:
        cooldown = score_result.false_breakout_cooldown or {}
        if not cooldown.get("active") or decision.action not in {Action.BUY, Action.TRIAL_BUY}:
            return decision
        notes = list(decision.notes)
        notes.append(
            "近期假突破冷却：同股短期多次入场失败，当前只保留观察和研究记录"
        )
        return replace(
            decision,
            action=Action.WATCH,
            position_pct=0.0,
            notes=notes,
        )

    def _score_payload_with_reference(self, score_result: ScoreResult) -> dict[str, Any]:
        payload = score_result.to_dict()
        if score_result.data_quality == DataQuality.DEGRADED:
            previous = self._previous_valid_score_reference(score_result.code)
            if previous:
                payload["previous_valid_score"] = previous
        return payload

    def _previous_valid_score_reference(self, code: str) -> dict[str, Any] | None:
        events = self._event_store.query(
            stream=f"strategy:{code}",
            event_type=SCORE_CALCULATED,
            limit=1000,
        )
        for event in reversed(events):
            payload = event.get("payload") or {}
            if payload.get("data_quality") != DataQuality.OK.value:
                continue
            total_score = payload.get("total_score")
            if total_score is None:
                continue
            metadata = event.get("metadata") or {}
            return {
                "event_id": event.get("event_id"),
                "occurred_at": event.get("occurred_at"),
                "run_id": metadata.get("run_id"),
                "config_version": metadata.get("config_version"),
                "total_score": total_score,
                "data_quality": DataQuality.OK.value,
                "reference_only": True,
                "note": "当前评分数据质量降级时仅作参考，不替代本次评分。",
            }
        return None


def _market_state_payload(market_state: MarketState) -> dict[str, Any]:
    return {
        "signal": market_state.signal.value,
        "multiplier": market_state.multiplier,
        "detail": market_state.detail,
    }


def _score_evidence_payload(score_payload: dict[str, Any]) -> dict[str, Any]:
    routes = score_payload.get("strategy_routes") or []
    primary_route = score_payload.get("primary_strategy_route")
    return {
        "entry_signal": bool(score_payload.get("entry_signal")),
        "primary_strategy_route": primary_route,
        "primary_strategy_route_label": _primary_route_label(routes, primary_route),
        "strategy_routes": routes,
        "technical_detail": score_payload.get("technical_detail", ""),
        "data_quality": score_payload.get("data_quality", ""),
        "false_breakout_cooldown": score_payload.get("false_breakout_cooldown", {}) or {},
    }


def _primary_route_label(routes: list[Any], primary_route: Any) -> str | None:
    for route in routes:
        if not isinstance(route, dict):
            continue
        if primary_route and route.get("route") != primary_route:
            continue
        label = route.get("display_name")
        if label:
            return str(label)
    return None


def _decision_rules_payload(decider: Decider) -> dict[str, Any]:
    return {
        "buy_threshold": decider.buy_threshold,
        "watch_threshold": decider.watch_threshold,
        "reject_threshold": decider.reject_threshold,
        "single_max_pct": decider.single_max_pct,
        "total_max_pct": decider.total_max_pct,
        "weekly_max": decider.weekly_max,
        "require_entry_signal_for_buy": decider.require_entry_signal_for_buy,
        "min_data_quality_for_buy": decider.min_data_quality_for_buy,
        "max_missing_fields_for_buy": decider.max_missing_fields_for_buy,
        "critical_missing_fields_for_buy": sorted(decider.critical_missing_fields_for_buy),
        "min_position_pct_for_buy": decider.min_position_pct_for_buy,
    }
