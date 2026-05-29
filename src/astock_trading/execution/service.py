"""
execution/service.py — 执行服务层

编排订单 + 持仓 + 投影，对外统一接口。
"""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from astock_trading.execution.models import Order, OrderSide, Position
from astock_trading.execution.orders import OrderManager
from astock_trading.execution.positions import (
    PositionManager,
    PositionProjector,
    allocate_cost_basis_cents,
)
from astock_trading.platform.domain_events import (
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_OUTCOME_RECORDED,
)
from astock_trading.platform.events import EventStore


@runtime_checkable
class BrokerAdapter(Protocol):
    """Broker 统一接口。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        """提交订单，返回 {"success": bool, "fill_price_cents": int, "fee_cents": int}。"""
        ...


class SimulatedBroker:
    """回测用 broker — 立即成交，无手续费。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        return {
            "success": True,
            "fill_price_cents": price_cents,
            "fee_cents": 0,
        }


class MXBroker:
    """妙想模拟盘 broker — 通过 MX API 下单。"""

    def submit_order(
        self, code: str, side: str, shares: int, price_cents: int
    ) -> dict:
        try:
            from astock_trading.market.mx.moni import dispatch_trade_command
            command = f"mx.moni.{side}"
            result = dispatch_trade_command(
                command,
                stock_code=code,
                quantity=shares,
                use_market_price=True,
            )
            trade_code = str(result.get("code", ""))
            if trade_code == "200":
                return {
                    "success": True,
                    "fill_price_cents": price_cents,  # MX 市价单，用请求价近似
                    "fee_cents": 0,
                    "broker_order_id": str(result.get("data", {}).get("orderId", "")),
                }
            else:
                return {
                    "success": False,
                    "error": result.get("message", f"MX API code={trade_code}"),
                }
        except Exception as e:
            return {"success": False, "error": str(e)}


class ExecutionService:
    """执行服务 — 统一入口。"""

    def __init__(
        self,
        event_store: EventStore,
        conn: Any,
        broker: Optional[BrokerAdapter] = None,
        on_trade: Optional[list] = None,
    ):
        self._events = event_store
        self._conn = conn
        self._orders = OrderManager(event_store, conn)
        self._positions = PositionManager(event_store, conn)
        self._projector = PositionProjector(event_store, conn)
        self._broker = broker or SimulatedBroker()
        self._on_trade = on_trade or []  # list of callable(trade_info: dict)

    # ------------------------------------------------------------------
    # 读操作（从投影表）
    # ------------------------------------------------------------------

    def get_positions(self) -> list[Position]:
        return self._positions.get_positions()

    def get_position(self, code: str) -> Optional[Position]:
        return self._positions.get_position(code)

    def get_portfolio(self) -> dict:
        """从投影表读取组合概览。"""
        positions = self.get_positions()
        total_cost = sum(p.effective_cost_basis_cents for p in positions)
        total_market = sum((p.current_price_cents or p.avg_cost_cents) * p.shares for p in positions)

        return {
            "holding_count": len(positions),
            "total_cost_cents": total_cost,
            "total_market_cents": total_market,
            "unrealized_pnl_cents": total_market - total_cost,
            "positions": [p.to_dict() for p in positions],
        }

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get_order(order_id)

    def audit_manual_trade_consistency(self, order_id: str) -> dict:
        """Check that a manually recorded filled order matches local projections."""
        issues: list[str] = []
        row = self._conn.execute(
            "SELECT * FROM projection_orders WHERE order_id = ?", (order_id,)
        ).fetchone()
        if not row:
            return {"ok": False, "issues": ["order_missing"], "order": None}

        order = dict(row)
        if order["broker"] != "manual":
            issues.append("order_not_manual")
        if order["status"] != "filled":
            issues.append("order_not_filled")

        pos_row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (order["code"],)
        ).fetchone()
        position = dict(pos_row) if pos_row else None

        if order["side"] == "buy":
            if position is None:
                issues.append("position_missing")
            else:
                if position["shares"] != order["shares"]:
                    issues.append("position_shares_mismatch")
                if position["avg_cost_cents"] != order["price_cents"]:
                    issues.append("position_cost_mismatch")
        elif order["side"] == "sell":
            if position is not None:
                trade_events = self._events.query(stream=f"trade:{order['code']}:{order_id}")
                outcome = next(
                    (
                        e["payload"]
                        for e in trade_events
                        if e["event_type"] == TRADE_OUTCOME_RECORDED
                    ),
                    None,
                )
                expected_after = outcome.get("position_after") if outcome else None
                if not expected_after:
                    issues.append("position_still_open_after_sell")
                elif position["shares"] != expected_after.get("shares"):
                    issues.append("position_after_shares_mismatch")
                elif (
                    position.get("cost_basis_cents")
                    and expected_after.get("cost_basis_cents")
                    and position["cost_basis_cents"] != expected_after["cost_basis_cents"]
                ):
                    issues.append("position_after_cost_basis_mismatch")
        else:
            issues.append("unknown_order_side")

        order_events = self._events.query(stream=f"order:{order['code']}:{order_id}")
        event_types = {e["event_type"] for e in order_events}
        if "order.created" not in event_types:
            issues.append("order_created_event_missing")
        if "order.filled" not in event_types:
            issues.append("order_filled_event_missing")

        position_events = self._events.query(stream=f"position:{order['code']}")
        position_event_types = {e["event_type"] for e in position_events}
        if order["side"] == "buy":
            if "position.opened" not in position_event_types:
                issues.append("position.opened_event_missing")
        elif order["side"] == "sell":
            required_event = "position.reduced" if position is not None else "position.closed"
            if required_event not in position_event_types:
                issues.append(f"{required_event}_event_missing")

        balance = self._conn.execute(
            "SELECT * FROM projection_balances WHERE scope = 'main'"
        ).fetchone()
        if not balance:
            issues.append("balance_missing")

        return {
            "ok": not issues,
            "issues": issues,
            "order": order,
            "position": position,
            "balance": dict(balance) if balance else None,
        }

    # ------------------------------------------------------------------
    # 写操作（事件化）
    # ------------------------------------------------------------------

    def execute_buy(
        self,
        code: str,
        name: str,
        shares: int,
        price_cents: int,
        style: str,
        run_id: str,
        broker: str = "",
    ) -> Order:
        """
        买入流程：创建订单 → 提交 broker → 成交 → 开仓。
        """
        order = self._orders.create_order(
            code=code, name=name, side=OrderSide.BUY,
            shares=shares, price_cents=price_cents,
            run_id=run_id, broker=broker,
        )

        result = self._broker.submit_order(code, "buy", shares, price_cents)

        if result["success"]:
            self._orders.fill_order(
                order.order_id,
                fill_price_cents=result["fill_price_cents"],
                fee_cents=result["fee_cents"],
                run_id=run_id,
            )
            self._positions.open_position(
                code=code, name=name, shares=shares,
                avg_cost_cents=result["fill_price_cents"],
                cost_basis_cents=result["fill_price_cents"] * shares + result.get("fee_cents", 0),
                style=style, run_id=run_id,
            )
            self._notify_trade({
                "side": "buy", "code": code, "name": name,
                "shares": shares, "price_cents": result["fill_price_cents"],
                "style": style, "run_id": run_id, "order_id": order.order_id,
            })
        else:
            self._orders.cancel_order(order.order_id, "broker_rejected", run_id)

        return order

    def execute_sell(
        self,
        code: str,
        shares: int,
        price_cents: int,
        run_id: str,
        reason: str = "",
        broker: str = "",
    ) -> Order:
        """
        卖出流程：创建订单 → 提交 broker → 成交 → 清仓。
        """
        pos = self._positions.get_position(code)
        name = pos.name if pos else code

        order = self._orders.create_order(
            code=code, name=name, side=OrderSide.SELL,
            shares=shares, price_cents=price_cents,
            run_id=run_id, broker=broker,
        )

        result = self._broker.submit_order(code, "sell", shares, price_cents)

        if result["success"]:
            self._orders.fill_order(
                order.order_id,
                fill_price_cents=result["fill_price_cents"],
                fee_cents=result["fee_cents"],
                run_id=run_id,
            )
            if pos:
                self._positions.close_position(
                    code=code, shares=shares,
                    sell_price_cents=result["fill_price_cents"],
                    run_id=run_id, reason=reason,
                    sell_fee_cents=result.get("fee_cents", 0),
                )
            self._notify_trade({
                "side": "sell", "code": code, "name": name,
                "shares": shares, "price_cents": result["fill_price_cents"],
                "reason": reason, "run_id": run_id, "order_id": order.order_id,
            })
        else:
            self._orders.cancel_order(order.order_id, "broker_rejected", run_id)

        return order

    def record_sell(
        self,
        code: str,
        shares: int,
        price_cents: int,
        fee_cents: int = 0,
        reason: str = "",
        run_id: str = "",
        source_event_id: str = "",
        source_score_event_id: str = "",
        hypothesis: dict | None = None,
    ) -> Order:
        """
        手动录入已成交的卖出记录（绕过 broker）。
        走事件化流程：order.created → order.filled → position.reduced/position.closed。
        fill_order 会同步更新现金（gross - fee）。
        """
        if run_id:
            rid = run_id
        else:
            import uuid
            rid = f"manual_sell_{code}_{uuid.uuid4().hex[:8]}"

        pos = self._positions.get_position(code)
        name = pos.name if pos else code

        if pos is None:
            raise ValueError(f"持仓不存在：{code}")

        if shares <= 0:
            raise ValueError(f"shares 必须 > 0，当前为 {shares}")

        if shares > pos.shares:
            raise ValueError(f"卖出股数不能超过当前持仓：{shares} > {pos.shares}")

        if price_cents <= 0:
            raise ValueError(f"price_cents 必须 > 0，当前为 {price_cents}")
        sold_cost_basis_cents = allocate_cost_basis_cents(
            pos.effective_cost_basis_cents,
            pos.shares,
            shares,
        )

        order = self._orders.create_order(
            code=code, name=name, side=OrderSide.SELL,
            shares=shares, price_cents=price_cents,
            run_id=rid, broker="manual",
        )
        self._record_trade_hypothesis(
            order_id=order.order_id,
            code=code,
            name=name,
            side="sell",
            shares=shares,
            price_cents=price_cents,
            fee_cents=fee_cents,
            reason=reason or "manual",
            run_id=rid,
            source_event_id=source_event_id,
            source_score_event_id=source_score_event_id,
            hypothesis=hypothesis,
        )

        # 成交录入（fill_order 会更新现金）
        self._orders.fill_order(
            order.order_id,
            fill_price_cents=price_cents,
            fee_cents=fee_cents,
            run_id=rid,
        )

        # 清仓
        realized_pnl_cents = self._positions.close_position(
            code=code, shares=shares,
            sell_price_cents=price_cents,
            run_id=rid, reason=reason or "manual",
            sell_fee_cents=fee_cents,
        )
        self._record_trade_outcome(
            order_id=order.order_id,
            code=code,
            name=name,
            side="sell",
            shares=shares,
            fill_price_cents=price_cents,
            fee_cents=fee_cents,
            reason=reason or "manual",
            run_id=rid,
            source_event_id=source_event_id,
            source_score_event_id=source_score_event_id,
            position_after=self._positions.get_position(code),
            cost_basis_cents=sold_cost_basis_cents,
            realized_pnl_cents=realized_pnl_cents,
        )

        self._notify_trade({
            "side": "sell", "code": code, "name": name,
            "shares": shares, "price_cents": price_cents,
            "reason": reason or "manual", "run_id": rid, "order_id": order.order_id,
        })

        return order

    def record_buy(
        self,
        code: str,
        shares: int,
        price_cents: int,
        fee_cents: int = 0,
        reason: str = "",
        run_id: str = "",
        name: str = "",
        style: str = "",
        source_event_id: str = "",
        source_score_event_id: str = "",
        hypothesis: dict | None = None,
        cost_basis_cents: int | None = None,
    ) -> Order:
        """
        手动录入已成交的买入记录（绕过 broker）。
        走事件化流程：order.created → order.filled → position.opened。
        fill_order 会同步更新现金（gross + fee）。
        """
        if run_id:
            rid = run_id
        else:
            import uuid
            rid = f"manual_buy_{code}_{uuid.uuid4().hex[:8]}"

        if shares <= 0:
            raise ValueError(f"shares 必须 > 0，当前为 {shares}")

        if price_cents <= 0:
            raise ValueError(f"price_cents 必须 > 0，当前为 {price_cents}")
        effective_cost_basis_cents = (
            cost_basis_cents
            if cost_basis_cents and cost_basis_cents > 0
            else price_cents * shares + fee_cents
        )

        from astock_trading.strategy.models import Style
        _style_map = {"growth": Style.MOMENTUM, "slow_bull": Style.SLOW_BULL, "momentum": Style.MOMENTUM}
        style_enum = _style_map.get(style.lower(), Style.UNKNOWN) if style else Style.UNKNOWN

        order = self._orders.create_order(
            code=code, name=name or code, side=OrderSide.BUY,
            shares=shares, price_cents=price_cents,
            run_id=rid, broker="manual",
        )
        self._record_trade_hypothesis(
            order_id=order.order_id,
            code=code,
            name=name or code,
            side="buy",
            shares=shares,
            price_cents=price_cents,
            fee_cents=fee_cents,
            cost_basis_cents=effective_cost_basis_cents,
            reason=reason or "manual",
            run_id=rid,
            source_event_id=source_event_id,
            source_score_event_id=source_score_event_id,
            hypothesis=hypothesis,
        )

        # 成交录入（fill_order 会更新现金）
        self._orders.fill_order(
            order.order_id,
            fill_price_cents=price_cents,
            fee_cents=fee_cents,
            run_id=rid,
        )

        # 开仓
        position = self._positions.open_position(
            code=code, name=name or code, shares=shares,
            avg_cost_cents=price_cents,
            cost_basis_cents=effective_cost_basis_cents,
            style=style_enum,
            run_id=rid,
        )
        self._record_trade_outcome(
            order_id=order.order_id,
            code=code,
            name=name or code,
            side="buy",
            shares=shares,
            fill_price_cents=price_cents,
            fee_cents=fee_cents,
            cost_basis_cents=effective_cost_basis_cents,
            reason=reason or "manual",
            run_id=rid,
            source_event_id=source_event_id,
            source_score_event_id=source_score_event_id,
            position_after=position,
        )

        self._notify_trade({
            "side": "buy", "code": code, "name": name or code,
            "shares": shares, "price_cents": price_cents,
            "reason": reason or "manual", "run_id": rid, "order_id": order.order_id,
        })

        return order

    def adjust_position_cost_basis(
        self,
        code: str,
        cost_basis_cents: int,
        reason: str = "",
        run_id: str = "",
    ) -> dict:
        """按券商成本价/总成本校准本地持仓成本。"""
        if run_id:
            rid = run_id
        else:
            import uuid
            rid = f"position_cost_adjust_{code}_{uuid.uuid4().hex[:8]}"
        before = self._positions.get_position(code)
        if before is None:
            raise ValueError(f"持仓不存在：{code}")
        event_id, after = self._positions.adjust_cost_basis(
            code=code,
            cost_basis_cents=cost_basis_cents,
            run_id=rid,
            reason=reason or "manual_cost_adjustment",
        )
        return {
            "event_id": event_id,
            "run_id": rid,
            "position_before": before,
            "position_after": after,
        }

    # ------------------------------------------------------------------
    # 重建
    # ------------------------------------------------------------------

    def rebuild_projections(self) -> list[Position]:
        """从 event_log 完全重建 projection_positions。"""
        return self._projector.rebuild()

    # ------------------------------------------------------------------
    # Post-trade hooks
    # ------------------------------------------------------------------

    def _notify_trade(self, trade_info: dict) -> None:
        """通知所有注册的 on_trade 回调。"""
        for callback in self._on_trade:
            try:
                callback(trade_info)
            except Exception:
                pass  # hook 失败不影响交易本身

    def _record_trade_hypothesis(
        self,
        *,
        order_id: str,
        code: str,
        name: str,
        side: str,
        shares: int,
        price_cents: int,
        fee_cents: int,
        cost_basis_cents: int | None = None,
        reason: str,
        run_id: str,
        source_event_id: str = "",
        source_score_event_id: str = "",
        hypothesis: dict | None = None,
    ) -> str:
        payload = {
            "order_id": order_id,
            "code": code,
            "name": name,
            "side": side,
            "shares": shares,
            "price_cents": price_cents,
            "fee_cents": fee_cents,
            "source_event_id": source_event_id,
            "source_score_event_id": source_score_event_id,
            "hypothesis": _normalize_hypothesis(hypothesis, reason),
        }
        if cost_basis_cents is not None:
            payload["cost_basis_cents"] = cost_basis_cents
        return self._events.append(
            stream=f"trade:{code}:{order_id}",
            stream_type="trade",
            event_type=TRADE_HYPOTHESIS_RECORDED,
            payload=payload,
            metadata={"run_id": run_id, "account": "main", "execution": "manual"},
        )

    def _record_trade_outcome(
        self,
        *,
        order_id: str,
        code: str,
        name: str,
        side: str,
        shares: int,
        fill_price_cents: int,
        fee_cents: int,
        cost_basis_cents: int | None = None,
        reason: str,
        run_id: str,
        source_event_id: str = "",
        source_score_event_id: str = "",
        position_after: Position | None = None,
        realized_pnl_cents: int | None = None,
    ) -> str:
        payload = {
            "order_id": order_id,
            "code": code,
            "name": name,
            "side": side,
            "status": "filled",
            "shares": shares,
            "fill_price_cents": fill_price_cents,
            "fee_cents": fee_cents,
            "reason": reason,
            "source_event_id": source_event_id,
            "source_score_event_id": source_score_event_id,
            "position_after": position_after.to_dict() if position_after else None,
        }
        if cost_basis_cents is not None:
            payload["cost_basis_cents"] = cost_basis_cents
        if realized_pnl_cents is not None:
            payload["realized_pnl_cents"] = realized_pnl_cents
        return self._events.append(
            stream=f"trade:{code}:{order_id}",
            stream_type="trade",
            event_type=TRADE_OUTCOME_RECORDED,
            payload=payload,
            metadata={"run_id": run_id, "account": "main", "execution": "manual"},
        )


def _normalize_hypothesis(hypothesis: dict | None, reason: str) -> dict:
    payload = dict(hypothesis or {})
    if reason and not payload.get("manual_reason"):
        payload["manual_reason"] = reason
    if reason and not payload.get("thesis"):
        payload["thesis"] = reason
    return payload
