"""
execution/orders.py — 订单管理（事件化）

每个操作追加事件到 event_log，同步更新 projection_orders。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from astock_trading.execution.models import Order, OrderSide, OrderStatus
from astock_trading.platform.events import EventStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_order_id() -> str:
    return f"ord_{uuid.uuid4().hex[:12]}"


class OrderManager:
    """订单管理 — 事件化 + 投影同步。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def create_order(
        self,
        code: str,
        name: str,
        side: OrderSide,
        shares: int,
        price_cents: int,
        run_id: str,
        broker: str = "",
    ) -> Order:
        """创建订单 → 追加 order.created 事件 → 更新投影。"""
        order_id = _new_order_id()
        now = _now_iso()

        order = Order(
            order_id=order_id,
            code=code,
            name=name,
            side=side,
            shares=shares,
            price_cents=price_cents,
            status=OrderStatus.PENDING,
            broker=broker,
            created_at=now,
        )

        # 追加事件
        self._events.append(
            stream=f"order:{code}:{order_id}",
            stream_type="order",
            event_type="order.created",
            payload={
                "order_id": order_id,
                "code": code,
                "name": name,
                "side": side.value,
                "shares": shares,
                "price_cents": price_cents,
                "broker": broker,
            },
            metadata={"run_id": run_id},
        )

        # 更新投影
        self._conn.execute(
            """REPLACE INTO projection_orders
               (order_id, code, side, shares, price_cents, status, broker, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, code, side.value, shares, price_cents, "pending", broker, now, now),
        )

        return order

    def fill_order(
        self,
        order_id: str,
        fill_price_cents: int,
        fee_cents: int,
        run_id: str,
    ) -> None:
        """成交 → 追加 order.filled 事件 → 更新投影。"""
        now = _now_iso()

        # 读取现有订单
        row = self._conn.execute(
            "SELECT * FROM projection_orders WHERE order_id = ?", (order_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Order {order_id} not found")
        if row["status"] == "filled":
            raise ValueError(f"Order {order_id} already filled")

        self._events.append(
            stream=f"order:{row['code']}:{order_id}",
            stream_type="order",
            event_type="order.filled",
            payload={
                "order_id": order_id,
                "code": row["code"],
                "side": row["side"],
                "shares": row["shares"],
                "fill_price_cents": fill_price_cents,
                "fee_cents": fee_cents,
            },
            metadata={"run_id": run_id},
        )

        self._conn.execute(
            """UPDATE projection_orders
               SET status = 'filled', filled_at = ?, updated_at = ?
               WHERE order_id = ?""",
            (now, now, order_id),
        )

        # 同步更新余额（UPSERT，防止 scope='main' 行不存在时静默失败）
        # total_asset_cents 由 rebuild_all() 从 (现金+持仓市值) 重建，此处只动 cash
        if row["side"] == "buy":
            cash_change = -(fill_price_cents * row["shares"] + fee_cents)  # 负数
        else:
            cash_change = fill_price_cents * row["shares"] - fee_cents     # 正数

        balance = self._conn.execute(
            "SELECT scope FROM projection_balances WHERE scope = 'main'"
        ).fetchone()
        if balance:
            self._conn.execute(
                """UPDATE projection_balances
                   SET cash_cents = cash_cents + ?, updated_at = ?
                   WHERE scope = 'main'""",
                (cash_change, now),
            )
        else:
            self._conn.execute(
                """INSERT INTO projection_balances
                   (scope, cash_cents, total_asset_cents, weekly_buy_count,
                    daily_pnl_cents, consecutive_loss_days, updated_at)
                   VALUES ('main', ?, ?, 0, 0, 0, ?)""",
                (cash_change, cash_change, now),
            )

    def cancel_order(
        self,
        order_id: str,
        reason: str,
        run_id: str,
    ) -> None:
        """取消 → 追加 order.cancelled 事件 → 更新投影。"""
        now = _now_iso()

        row = self._conn.execute(
            "SELECT * FROM projection_orders WHERE order_id = ?", (order_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Order {order_id} not found")

        self._events.append(
            stream=f"order:{row['code']}:{order_id}",
            stream_type="order",
            event_type="order.cancelled",
            payload={"order_id": order_id, "reason": reason},
            metadata={"run_id": run_id},
        )

        self._conn.execute(
            "UPDATE projection_orders SET status = 'cancelled', updated_at = ? WHERE order_id = ?",
            (now, order_id),
        )

    def get_order(self, order_id: str) -> Optional[Order]:
        """从投影表读取订单。"""
        row = self._conn.execute(
            "SELECT * FROM projection_orders WHERE order_id = ?", (order_id,)
        ).fetchone()
        if not row:
            return None
        return Order(
            order_id=row["order_id"],
            code=row["code"],
            name="",
            side=OrderSide(row["side"]),
            shares=row["shares"],
            price_cents=row["price_cents"],
            status=OrderStatus(row["status"]),
            broker=row["broker"] or "",
            created_at=row["created_at"],
            filled_at=row["filled_at"],
        )
