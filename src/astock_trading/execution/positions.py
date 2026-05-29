"""
execution/positions.py — 持仓投影（从 event_log 重建）

持仓状态 = f(position.* 事件序列)。
投影表只是缓存，可随时删除后从 event_log 重建。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from astock_trading.execution.models import Position
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import iso_to_local, local_today_str, utc_now_iso


def _now_iso() -> str:
    return utc_now_iso()


def _effective_cost_basis_cents(
    shares: int,
    avg_cost_cents: int,
    cost_basis_cents: int | None = None,
) -> int:
    if cost_basis_cents and cost_basis_cents > 0:
        return cost_basis_cents
    return avg_cost_cents * shares


def allocate_cost_basis_cents(
    total_cost_basis_cents: int,
    total_shares: int,
    shares: int,
) -> int:
    """按股数比例分摊持仓总成本，返回本次卖出对应的成本金额。"""
    if total_shares <= 0:
        raise ValueError(f"total_shares 必须 > 0，当前为 {total_shares}")
    if shares <= 0:
        raise ValueError(f"shares 必须 > 0，当前为 {shares}")
    if shares > total_shares:
        raise ValueError(f"shares 不能大于当前持仓：{shares} > {total_shares}")
    if shares == total_shares:
        return total_cost_basis_cents
    return (total_cost_basis_cents * shares + total_shares // 2) // total_shares


class PositionManager:
    """持仓管理 — 事件化 + 投影同步。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def open_position(
        self,
        code: str,
        name: str,
        shares: int,
        avg_cost_cents: int,
        style: str,
        run_id: str,
        entry_day_low_cents: int = 0,
        currency: str = "CNY",
        cost_basis_cents: int | None = None,
    ) -> Position:
        """开仓 → 追加 position.opened 事件 → 更新投影。"""
        now = _now_iso()
        today = local_today_str()
        effective_cost_basis_cents = _effective_cost_basis_cents(
            shares,
            avg_cost_cents,
            cost_basis_cents,
        )
        unrealized_pnl_cents = avg_cost_cents * shares - effective_cost_basis_cents

        self._events.append(
            stream=f"position:{code}",
            stream_type="position",
            event_type="position.opened",
            payload={
                "code": code,
                "name": name,
                "shares": shares,
                "avg_cost_cents": avg_cost_cents,
                "cost_basis_cents": effective_cost_basis_cents,
                "style": style,
                "entry_day_low_cents": entry_day_low_cents,
                "currency": currency,
            },
            metadata={"run_id": run_id},
        )

        pos = Position(
            code=code, name=name, style=style,
            shares=shares, avg_cost_cents=avg_cost_cents,
            entry_date=today,
            cost_basis_cents=effective_cost_basis_cents,
            entry_day_low_cents=entry_day_low_cents,
            highest_since_entry_cents=avg_cost_cents,
            current_price_cents=avg_cost_cents,
            unrealized_pnl_cents=unrealized_pnl_cents,
            updated_at=now,
            currency=currency,
        )

        self._conn.execute(
            """INSERT OR REPLACE INTO projection_positions
               (code, name, style, shares, avg_cost_cents, cost_basis_cents, entry_date,
                entry_day_low_cents, highest_since_entry_cents,
                current_price_cents, unrealized_pnl_cents, updated_at, currency)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (code, name, style, shares, avg_cost_cents, effective_cost_basis_cents, today,
             entry_day_low_cents, avg_cost_cents, avg_cost_cents,
             unrealized_pnl_cents, now, currency),
        )

        return pos

    def close_position(
        self,
        code: str,
        shares: int,
        sell_price_cents: int,
        run_id: str,
        reason: str = "",
        sell_fee_cents: int = 0,
    ) -> int:
        """卖出持仓，支持全仓和部分卖出。返回 realized_pnl_cents。"""
        row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (code,)
        ).fetchone()
        if not row:
            raise ValueError(f"Position {code} not found")
        if shares <= 0:
            raise ValueError(f"shares 必须 > 0，当前为 {shares}")
        if sell_price_cents <= 0:
            raise ValueError(f"sell_price_cents 必须 > 0，当前为 {sell_price_cents}")
        if sell_fee_cents < 0:
            raise ValueError(f"sell_fee_cents 不能为负，当前为 {sell_fee_cents}")

        held_shares = int(row["shares"])
        avg_cost_cents = row["avg_cost_cents"]
        cost_basis_cents = _effective_cost_basis_cents(
            held_shares,
            avg_cost_cents,
            row["cost_basis_cents"] if "cost_basis_cents" in row.keys() else None,
        )
        sold_cost_basis_cents = allocate_cost_basis_cents(
            cost_basis_cents,
            held_shares,
            shares,
        )
        remaining_shares = held_shares - shares
        remaining_cost_basis_cents = cost_basis_cents - sold_cost_basis_cents
        holding_days = 0
        try:
            entry = datetime.strptime(row["entry_date"], "%Y-%m-%d").date()
            holding_days = (iso_to_local(_now_iso()).date() - entry).days
        except (ValueError, TypeError):
            pass

        realized_pnl_cents = sell_price_cents * shares - sell_fee_cents - sold_cost_basis_cents
        event_type = "position.closed" if remaining_shares == 0 else "position.reduced"
        now = _now_iso()

        self._events.append(
            stream=f"position:{code}",
            stream_type="position",
            event_type=event_type,
            payload={
                "code": code,
                "shares": shares,
                "sell_price_cents": sell_price_cents,
                "avg_cost_cents": avg_cost_cents,
                "cost_basis_cents": sold_cost_basis_cents,
                "sell_fee_cents": sell_fee_cents,
                "realized_pnl_cents": realized_pnl_cents,
                "holding_days": holding_days,
                "reason": reason,
                "position_before": {
                    "shares": held_shares,
                    "cost_basis_cents": cost_basis_cents,
                },
                "remaining_shares": remaining_shares,
                "remaining_cost_basis_cents": remaining_cost_basis_cents,
            },
            metadata={"run_id": run_id},
        )

        if remaining_shares == 0:
            self._conn.execute("DELETE FROM projection_positions WHERE code = ?", (code,))
        else:
            highest_since_entry_cents = max(
                int(row["highest_since_entry_cents"] or 0),
                sell_price_cents,
            )
            unrealized_pnl_cents = sell_price_cents * remaining_shares - remaining_cost_basis_cents
            self._conn.execute(
                """UPDATE projection_positions
                   SET shares = ?,
                       cost_basis_cents = ?,
                       highest_since_entry_cents = ?,
                       current_price_cents = ?,
                       unrealized_pnl_cents = ?,
                       updated_at = ?
                   WHERE code = ?""",
                (
                    remaining_shares,
                    remaining_cost_basis_cents,
                    highest_since_entry_cents,
                    sell_price_cents,
                    unrealized_pnl_cents,
                    now,
                    code,
                ),
            )
        return realized_pnl_cents

    def adjust_cost_basis(
        self,
        code: str,
        cost_basis_cents: int,
        run_id: str,
        reason: str = "",
    ) -> tuple[str, Position]:
        """调整持仓总成本，记录事件并同步投影。"""
        row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (code,)
        ).fetchone()
        if not row:
            raise ValueError(f"Position {code} not found")
        if cost_basis_cents <= 0:
            raise ValueError(f"cost_basis_cents 必须 > 0，当前为 {cost_basis_cents}")

        shares = int(row["shares"])
        price_cents = int(row["current_price_cents"] or row["avg_cost_cents"] or 0)
        unrealized_pnl_cents = price_cents * shares - cost_basis_cents
        now = _now_iso()
        event_id = self._events.append(
            stream=f"position:{code}",
            stream_type="position",
            event_type="position.cost_basis_adjusted",
            payload={
                "code": code,
                "cost_basis_cents": cost_basis_cents,
                "reason": reason,
            },
            metadata={"run_id": run_id},
        )
        self._conn.execute(
            """UPDATE projection_positions
               SET cost_basis_cents = ?,
                   unrealized_pnl_cents = ?,
                   updated_at = ?
               WHERE code = ?""",
            (cost_basis_cents, unrealized_pnl_cents, now, code),
        )
        position = self.get_position(code)
        if position is None:
            raise ValueError(f"Position {code} not found")
        return event_id, position

    def get_positions(self) -> list[Position]:
        """从投影表读取所有持仓。"""
        rows = self._conn.execute(
            "SELECT * FROM projection_positions ORDER BY entry_date"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def get_position(self, code: str) -> Optional[Position]:
        """从投影表读取单个持仓。"""
        row = self._conn.execute(
            "SELECT * FROM projection_positions WHERE code = ?", (code,)
        ).fetchone()
        return self._row_to_position(row) if row else None

    @staticmethod
    def _row_to_position(row: Any) -> Position:
        return Position(
            code=row["code"],
            name=row["name"],
            style=row["style"],
            shares=row["shares"],
            avg_cost_cents=row["avg_cost_cents"],
            entry_date=row["entry_date"],
            cost_basis_cents=_effective_cost_basis_cents(
                row["shares"],
                row["avg_cost_cents"],
                row["cost_basis_cents"] if "cost_basis_cents" in row.keys() else None,
            ),
            entry_day_low_cents=row["entry_day_low_cents"] or 0,
            stop_loss_cents=row["stop_loss_cents"] or 0,
            take_profit_cents=row["take_profit_cents"] or 0,
            highest_since_entry_cents=row["highest_since_entry_cents"] or 0,
            current_price_cents=row["current_price_cents"] or 0,
            unrealized_pnl_cents=row["unrealized_pnl_cents"] or 0,
            updated_at=row["updated_at"],
            currency=row["currency"] if "currency" in row.keys() else "CNY",
        )


class PositionProjector:
    """从 event_log 重建持仓投影。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def rebuild(self) -> list[Position]:
        """
        删除 projection_positions，从 event_log 完全重建。

        遍历所有 position.* 事件，按 stream 分组重放：
        - position.opened → 创建持仓
        - position.reduced → 按比例减仓
        - position.closed → 删除持仓
        """
        self._conn.execute("DELETE FROM projection_positions")

        # 查询所有 position 事件
        events = self._events.query(stream_type="position")

        # 按 stream 分组
        streams: dict[str, list[dict]] = {}
        for ev in events:
            s = ev["stream"]
            streams.setdefault(s, []).append(ev)

        positions: list[Position] = []

        for stream, evts in streams.items():
            # 按 version 排序
            evts.sort(key=lambda e: e.get("stream_version", 0))

            pos = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "position.opened":
                    cost_basis_cents = _effective_cost_basis_cents(
                        p["shares"],
                        p["avg_cost_cents"],
                        p.get("cost_basis_cents"),
                    )
                    pos = Position(
                        code=p["code"],
                        name=p.get("name", p["code"]),
                        style=p.get("style", "unknown"),
                        shares=p["shares"],
                        avg_cost_cents=p["avg_cost_cents"],
                        entry_date=ev.get("occurred_at", "")[:10],
                        cost_basis_cents=cost_basis_cents,
                        entry_day_low_cents=p.get("entry_day_low_cents", 0),
                        highest_since_entry_cents=p.get("avg_cost_cents", 0),
                        current_price_cents=p.get("avg_cost_cents", 0),
                        unrealized_pnl_cents=(
                            p.get("avg_cost_cents", 0) * p["shares"] - cost_basis_cents
                        ),
                        updated_at=ev.get("occurred_at", ""),
                        currency=p.get("currency", "CNY"),
                    )
                elif et == "position.cost_basis_adjusted" and pos is not None:
                    pos.cost_basis_cents = _effective_cost_basis_cents(
                        pos.shares,
                        pos.avg_cost_cents,
                        p.get("cost_basis_cents"),
                    )
                    pos.unrealized_pnl_cents = (
                        (pos.current_price_cents or pos.avg_cost_cents) * pos.shares
                        - pos.cost_basis_cents
                    )
                    pos.updated_at = ev.get("occurred_at", "")
                elif et == "position.reduced" and pos is not None:
                    remaining_shares = int(
                        p.get("remaining_shares", pos.shares - int(p.get("shares", 0)))
                    )
                    if remaining_shares <= 0:
                        pos = None
                        continue
                    sold_cost_basis_cents = p.get("cost_basis_cents")
                    remaining_cost_basis_cents = p.get("remaining_cost_basis_cents")
                    if remaining_cost_basis_cents is None:
                        if sold_cost_basis_cents is None:
                            sold_cost_basis_cents = allocate_cost_basis_cents(
                                pos.effective_cost_basis_cents,
                                pos.shares,
                                int(p.get("shares", 0)),
                            )
                        remaining_cost_basis_cents = (
                            pos.effective_cost_basis_cents - int(sold_cost_basis_cents)
                        )
                    current_price_cents = int(
                        p.get("remaining_current_price_cents")
                        or p.get("sell_price_cents")
                        or pos.current_price_cents
                        or pos.avg_cost_cents
                    )
                    pos.shares = remaining_shares
                    pos.cost_basis_cents = int(remaining_cost_basis_cents)
                    pos.current_price_cents = current_price_cents
                    pos.highest_since_entry_cents = max(
                        pos.highest_since_entry_cents,
                        current_price_cents,
                    )
                    pos.unrealized_pnl_cents = (
                        current_price_cents * pos.shares - pos.cost_basis_cents
                    )
                    pos.updated_at = ev.get("occurred_at", "")
                elif et == "position.closed":
                    pos = None  # 已清仓

            if pos is not None:
                # 写入投影
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_positions
                       (code, name, style, shares, avg_cost_cents, cost_basis_cents, entry_date,
                        entry_day_low_cents, highest_since_entry_cents,
                        current_price_cents, unrealized_pnl_cents, updated_at, currency)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pos.code, pos.name, pos.style, pos.shares,
                     pos.avg_cost_cents, pos.effective_cost_basis_cents, pos.entry_date,
                     pos.entry_day_low_cents, pos.highest_since_entry_cents,
                     pos.current_price_cents, pos.unrealized_pnl_cents,
                     pos.updated_at, pos.currency),
                )
                positions.append(pos)

        return positions
