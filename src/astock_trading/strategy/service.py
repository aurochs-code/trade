"""
strategy/service.py — 策略服务层

编排评分 + 决策，结果写入 event_log。
这是 strategy context 唯一允许做 IO（写事件）的地方。
"""

from __future__ import annotations


from astock_trading.market.models import StockSnapshot
from astock_trading.platform.events import EventStore
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.models import Action, DecisionIntent, MarketState, ScoreResult
from astock_trading.strategy.scorer import Scorer


class StrategyService:
    """编排评分 + 决策，结果追加到 event_log。"""

    def __init__(
        self,
        scorer: Scorer,
        decider: Decider,
        event_store: EventStore,
    ):
        self._scorer = scorer
        self._decider = decider
        self._event_store = event_store

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
            # 追加评分事件
            self._event_store.append(
                stream=f"strategy:{score_result.code}",
                stream_type="strategy",
                event_type="score.calculated",
                payload=score_result.to_dict(),
                metadata=metadata,
            )

            # 决策
            decision = self._decider.decide(
                score_result,
                market_state,
                current_exposure_pct=current_exposure_pct,
                weekly_buy_count=weekly_buy_count,
            )
            decisions.append(decision)

            # 追加决策事件
            decision_event_id = self._event_store.append(
                stream=f"strategy:{decision.code}",
                stream_type="strategy",
                event_type="decision.suggested",
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
                },
                metadata=metadata,
            )

            if decision.action == Action.BUY:
                snapshot = next((s for s in snapshots if s.code == decision.code), None)
                quote = snapshot.quote if snapshot else None
                self._event_store.append(
                    stream=f"manual_trade:{decision.code}",
                    stream_type="manual_trade",
                    event_type="manual_trade.requested",
                    payload={
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
                    },
                    metadata={**metadata, "account": "main", "execution": "manual"},
                )

        return decisions

    def score_single(
        self,
        snapshot: StockSnapshot,
        run_id: str,
        config_version: str,
    ) -> ScoreResult:
        """单股评分，结果追加到 event_log。"""
        result = self._scorer.score(snapshot)

        self._event_store.append(
            stream=f"strategy:{result.code}",
            stream_type="strategy",
            event_type="score.calculated",
            payload=result.to_dict(),
            metadata={"run_id": run_id, "config_version": config_version},
        )

        return result
