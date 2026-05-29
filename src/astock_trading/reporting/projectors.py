"""
reporting/projectors.py — 投影更新器

从 event_log 同步更新所有 projection 表。
reporting 只读消费事实，不反写业务表。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from astock_trading.platform.events import EventStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProjectionUpdater:
    """从 event_log 同步更新所有 projection 表。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def rebuild_all(self) -> dict:
        """删除所有 projection 数据，从 event_log 完全重建。"""
        self._clear_projection_tables()
        stats = {}
        stats["positions"] = self._rebuild_positions()
        stats["orders"] = self._rebuild_orders()
        stats["balances"] = self._rebuild_balances()
        stats["candidate_pool"] = self._rebuild_candidate_pool()
        stats["market_state"] = self._rebuild_market_state()
        stats["report_artifacts"] = self._rebuild_report_artifacts()
        return stats

    def sync_all(self, since: Optional[str] = None) -> dict:
        """增量同步（简化版：目前等同于 rebuild）。"""
        return self.rebuild_all()

    def _clear_projection_tables(self) -> None:
        """先清空所有可重放 projection，避免部分重建时留下陈旧行。"""
        for table in (
            "projection_positions",
            "projection_orders",
            "projection_balances",
            "projection_candidate_pool",
            "projection_market_state",
            "report_artifacts",
        ):
            self._conn.execute(f"DELETE FROM {table}")

    def _all_events(self) -> list[dict]:
        return self._events.query(limit=1_000_000)

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------

    def _rebuild_positions(self) -> int:
        """从 position.* 事件重建 projection_positions。"""
        self._conn.execute("DELETE FROM projection_positions")

        events = self._events.query(stream_type="position", limit=1_000_000)
        streams: dict[str, list[dict]] = {}
        for ev in events:
            streams.setdefault(ev["stream"], []).append(ev)

        count = 0
        for stream, evts in streams.items():
            evts.sort(key=lambda e: e.get("stream_version", 0))

            pos_data = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "position.opened":
                    cost_basis_cents = p.get("cost_basis_cents") or p["avg_cost_cents"] * p["shares"]
                    pos_data = {
                        "code": p["code"],
                        "name": p.get("name", p["code"]),
                        "style": p.get("style", "unknown"),
                        "shares": p["shares"],
                        "avg_cost_cents": p["avg_cost_cents"],
                        "cost_basis_cents": cost_basis_cents,
                        "entry_date": ev.get("occurred_at", "")[:10],
                        "entry_day_low_cents": p.get("entry_day_low_cents", 0),
                        "highest_since_entry_cents": p.get("avg_cost_cents", 0),
                        "current_price_cents": p.get("avg_cost_cents", 0),
                        "unrealized_pnl_cents": p.get("avg_cost_cents", 0) * p["shares"] - cost_basis_cents,
                        "updated_at": ev.get("occurred_at", ""),
                    }
                elif et == "position.cost_basis_adjusted" and pos_data:
                    pos_data["cost_basis_cents"] = p.get("cost_basis_cents") or pos_data["cost_basis_cents"]
                    pos_data["unrealized_pnl_cents"] = (
                        (pos_data["current_price_cents"] or pos_data["avg_cost_cents"]) * pos_data["shares"]
                        - pos_data["cost_basis_cents"]
                    )
                    pos_data["updated_at"] = ev.get("occurred_at", "")
                elif et == "position.reduced" and pos_data:
                    remaining_shares = int(
                        p.get("remaining_shares", pos_data["shares"] - int(p.get("shares", 0)))
                    )
                    if remaining_shares <= 0:
                        pos_data = None
                        continue
                    sold_cost_basis_cents = p.get("cost_basis_cents")
                    remaining_cost_basis_cents = p.get("remaining_cost_basis_cents")
                    if remaining_cost_basis_cents is None:
                        if sold_cost_basis_cents is None:
                            sold_cost_basis_cents = (
                                pos_data["cost_basis_cents"] * int(p.get("shares", 0))
                                + pos_data["shares"] // 2
                            ) // pos_data["shares"]
                        remaining_cost_basis_cents = (
                            pos_data["cost_basis_cents"] - int(sold_cost_basis_cents)
                        )
                    current_price_cents = int(
                        p.get("remaining_current_price_cents")
                        or p.get("sell_price_cents")
                        or pos_data["current_price_cents"]
                        or pos_data["avg_cost_cents"]
                    )
                    pos_data["shares"] = remaining_shares
                    pos_data["cost_basis_cents"] = int(remaining_cost_basis_cents)
                    pos_data["current_price_cents"] = current_price_cents
                    pos_data["highest_since_entry_cents"] = max(
                        pos_data["highest_since_entry_cents"],
                        current_price_cents,
                    )
                    pos_data["unrealized_pnl_cents"] = (
                        current_price_cents * remaining_shares - pos_data["cost_basis_cents"]
                    )
                    pos_data["updated_at"] = ev.get("occurred_at", "")
                elif et == "position.closed":
                    pos_data = None

            if pos_data:
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_positions
                       (code, name, style, shares, avg_cost_cents, cost_basis_cents, entry_date,
                        entry_day_low_cents, highest_since_entry_cents,
                        current_price_cents, unrealized_pnl_cents, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pos_data["code"], pos_data["name"], pos_data["style"],
                     pos_data["shares"], pos_data["avg_cost_cents"],
                     pos_data["cost_basis_cents"],
                     pos_data["entry_date"], pos_data["entry_day_low_cents"],
                     pos_data["highest_since_entry_cents"],
                     pos_data["current_price_cents"],
                     pos_data["unrealized_pnl_cents"], pos_data["updated_at"]),
                )
                count += 1

        return count

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    def _rebuild_orders(self) -> int:
        """从 order.* 事件重建 projection_orders。"""
        self._conn.execute("DELETE FROM projection_orders")

        events = self._events.query(stream_type="order", limit=1_000_000)
        streams: dict[str, list[dict]] = {}
        for ev in events:
            streams.setdefault(ev["stream"], []).append(ev)

        count = 0
        for stream, evts in streams.items():
            evts.sort(key=lambda e: e.get("stream_version", 0))

            order_data = None
            for ev in evts:
                et = ev["event_type"]
                p = ev["payload"]

                if et == "order.created":
                    order_data = {
                        "order_id": p["order_id"],
                        "code": p["code"],
                        "side": p["side"],
                        "shares": p["shares"],
                        "price_cents": p["price_cents"],
                        "status": "pending",
                        "broker": p.get("broker", ""),
                        "created_at": ev.get("occurred_at", ""),
                        "filled_at": None,
                        "updated_at": ev.get("occurred_at", ""),
                    }
                elif et == "order.filled" and order_data:
                    order_data["status"] = "filled"
                    order_data["filled_at"] = ev.get("occurred_at", "")
                    order_data["updated_at"] = ev.get("occurred_at", "")
                elif et == "order.cancelled" and order_data:
                    order_data["status"] = "cancelled"
                    order_data["updated_at"] = ev.get("occurred_at", "")

            if order_data:
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_orders
                       (order_id, code, side, shares, price_cents, status,
                        broker, created_at, filled_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (order_data["order_id"], order_data["code"],
                     order_data["side"], order_data["shares"],
                     order_data["price_cents"], order_data["status"],
                     order_data["broker"], order_data["created_at"],
                     order_data["filled_at"], order_data["updated_at"]),
                )
                count += 1

        return count

    # ------------------------------------------------------------------
    # Market State
    # ------------------------------------------------------------------

    def sync_market_state(self, index_data: dict[str, dict]) -> int:
        """从指数数据同步 projection_market_state。"""
        now = _now_iso()
        count = 0

        for name, data in index_data.items():
            if "error" in data:
                continue
            symbol = data.get("symbol", name)
            price = data.get("price") or data.get("close")
            change_pct = data.get("change_pct", 0)
            # 计算 MA 相对位置百分比（price 可能为 0，但 ma20/ma60 本身来自日线数据）
            ma20 = data.get("ma20", 0)
            ma60 = data.get("ma60", 0)
            ma20_pct = ((price / ma20 - 1) * 100) if price and ma20 and ma20 > 0 else None
            ma60_pct = ((price / ma60 - 1) * 100) if price and ma60 and ma60 > 0 else None
            
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_market_state
                   (index_symbol, name, `signal`, price_cents, change_pct,
                    ma20_pct, ma60_pct, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol, name,
                    data.get("signal", ""),
                    int(price * 100) if price else None,
                    change_pct,
                    ma20_pct,
                    ma60_pct,
                    now,
                ),
            )
            count += 1

        return count

    def _rebuild_market_state(self) -> int:
        """从 market.* 快照事件重建 projection_market_state；无事件时保持空表并返回 0。"""
        self._conn.execute("DELETE FROM projection_market_state")

        count = 0
        for ev in self._all_events():
            if not self._is_market_state_event(ev):
                continue
            for name, data in self._market_entries_from_payload(ev["payload"]):
                if "error" in data:
                    continue
                symbol = data.get("index_symbol") or data.get("symbol") or name
                price = data.get("price") or data.get("close")
                ma20 = data.get("ma20", 0)
                ma60 = data.get("ma60", 0)
                ma20_pct = ((price / ma20 - 1) * 100) if price and ma20 and ma20 > 0 else None
                ma60_pct = ((price / ma60 - 1) * 100) if price and ma60 and ma60 > 0 else None
                self._conn.execute(
                    """INSERT OR REPLACE INTO projection_market_state
                       (index_symbol, name, `signal`, price_cents, change_pct,
                        ma20_pct, ma60_pct, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        symbol,
                        data.get("name", name),
                        data.get("signal", ""),
                        int(price * 100) if price else None,
                        data.get("change_pct", 0),
                        data.get("ma20_pct", ma20_pct),
                        data.get("ma60_pct", ma60_pct),
                        data.get("updated_at", ev.get("occurred_at", "")),
                    ),
                )
                count += 1

        return count

    @staticmethod
    def _is_market_state_event(ev: dict) -> bool:
        event_type = ev.get("event_type", "")
        return event_type in {
            "market.state.updated",
            "market.state.synced",
            "market.state.snapshot",
            "market.index.updated",
            "market.index.snapshot",
        }

    @staticmethod
    def _market_entries_from_payload(payload: dict) -> list[tuple[str, dict]]:
        indices = payload.get("indices") or payload.get("index_data")
        if isinstance(indices, dict):
            return [(name, data) for name, data in indices.items() if isinstance(data, dict)]
        if isinstance(indices, list):
            entries = []
            for data in indices:
                if isinstance(data, dict):
                    entries.append((data.get("name") or data.get("symbol") or "", data))
            return entries
        if payload.get("symbol") or payload.get("index_symbol"):
            return [(payload.get("name") or payload.get("symbol") or payload.get("index_symbol"), payload)]
        return []

    # ------------------------------------------------------------------
    # Candidate Pool
    # ------------------------------------------------------------------

    def sync_candidate_pool(self, entries: list[dict]) -> int:
        """从评分结果同步 projection_candidate_pool。"""
        now = _now_iso()
        count = 0

        for entry in entries:
            code = entry.get("code", "")
            if not code:
                continue
            tier = entry.get("pool_tier", entry.get("bucket", "watch"))
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_candidate_pool
                   (code, pool_tier, name, score, added_at, last_scored_at,
                    streak_days, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    code, tier,
                    entry.get("name", ""),
                    entry.get("score", entry.get("total_score")),
                    entry.get("added_at", now[:10]),
                    now[:10],
                    entry.get("streak_days", 0),
                    entry.get("note", ""),
                ),
            )
            count += 1

        return count

    def _rebuild_candidate_pool(self) -> int:
        """从 candidate.* 和 pool.* 事件重建 projection_candidate_pool。"""
        self._conn.execute("DELETE FROM projection_candidate_pool")

        candidates: dict[str, dict] = {}
        for ev in self._all_events():
            event_type = ev.get("event_type", "")
            if event_type not in {
                "candidate.added",
                "candidate.promoted",
                "candidate.updated",
                "candidate.rejected",
                "pool.demoted",
                "pool.removed",
            }:
                continue

            payload = ev["payload"]
            code = payload.get("code")
            if not code:
                continue

            if event_type in {"candidate.rejected", "pool.removed"}:
                candidates.pop(code, None)
                continue

            if event_type == "pool.demoted":
                current = candidates.get(code, {})
                candidates[code] = {
                    "code": code,
                    "pool_tier": payload.get("to") or payload.get("pool_tier") or "watch",
                    "name": payload.get("name", current.get("name", "")),
                    "score": payload.get("score", current.get("score")),
                    "added_at": current.get("added_at", ev.get("occurred_at", "")[:10]),
                    "last_scored_at": ev.get("occurred_at", "")[:10],
                    "streak_days": payload.get("streak_days", current.get("streak_days", 0)),
                    "note": payload.get("reason", payload.get("note", current.get("note", ""))),
                }
                continue

            candidates[code] = {
                "code": code,
                "pool_tier": payload.get("pool_tier", payload.get("bucket", "watch")),
                "name": payload.get("name", ""),
                "score": payload.get("score", payload.get("total_score")),
                "added_at": payload.get("added_at", ev.get("occurred_at", "")[:10]),
                "last_scored_at": payload.get("last_scored_at", ev.get("occurred_at", "")[:10]),
                "streak_days": payload.get("streak_days", 0),
                "note": payload.get("note", ""),
            }

        count = 0
        for entry in candidates.values():
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_candidate_pool
                   (code, pool_tier, name, score, added_at, last_scored_at,
                    streak_days, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    entry["code"],
                    entry["pool_tier"],
                    entry["name"],
                    entry["score"],
                    entry["added_at"],
                    entry["last_scored_at"],
                    entry["streak_days"],
                    entry["note"],
                ),
            )
            count += 1

        return count

    # ------------------------------------------------------------------
    # Balances
    # ------------------------------------------------------------------

    def sync_balances(
        self,
        scope: str,
        cash_cents: int,
        total_asset_cents: int,
        weekly_buy_count: int = 0,
        daily_pnl_cents: int = 0,
        consecutive_loss_days: int = 0,
    ) -> None:
        """同步 projection_balances。"""
        now = _now_iso()
        self._conn.execute(
            """INSERT OR REPLACE INTO projection_balances
               (scope, cash_cents, total_asset_cents, weekly_buy_count,
                daily_pnl_cents, consecutive_loss_days, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (scope, cash_cents, total_asset_cents, weekly_buy_count,
             daily_pnl_cents, consecutive_loss_days, now),
        )

    def _rebuild_balances(self) -> int:
        """从 balance.* 快照事件重建 projection_balances。"""
        self._conn.execute("DELETE FROM projection_balances")

        balances: dict[str, dict] = {}
        for ev in self._all_events():
            if not ev.get("event_type", "").startswith("balance."):
                continue

            payload = ev["payload"]
            scope = payload.get("scope") or ev.get("stream", "balance:main").split(":", 1)[-1]
            balances[scope] = {
                "scope": scope,
                "cash_cents": self._payload_cents(payload, "cash_cents", "cash", "available_cash"),
                "total_asset_cents": self._payload_cents(
                    payload, "total_asset_cents", "total_asset", "total"
                ),
                "weekly_buy_count": int(payload.get("weekly_buy_count", 0) or 0),
                "daily_pnl_cents": int(payload.get("daily_pnl_cents", 0) or 0),
                "consecutive_loss_days": int(payload.get("consecutive_loss_days", 0) or 0),
                "updated_at": payload.get("updated_at", ev.get("occurred_at", "")),
            }

        for row in balances.values():
            self._conn.execute(
                """INSERT OR REPLACE INTO projection_balances
                   (scope, cash_cents, total_asset_cents, weekly_buy_count,
                    daily_pnl_cents, consecutive_loss_days, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    row["scope"],
                    row["cash_cents"],
                    row["total_asset_cents"],
                    row["weekly_buy_count"],
                    row["daily_pnl_cents"],
                    row["consecutive_loss_days"],
                    row["updated_at"],
                ),
            )

        return len(balances)

    @staticmethod
    def _payload_cents(payload: dict, cents_key: str, *money_keys: str) -> int | None:
        if cents_key in payload:
            value = payload[cents_key]
            return int(value) if value is not None else None
        for key in money_keys:
            if key in payload:
                value = payload[key]
                return int(round(float(value) * 100)) if value is not None else None
        return 0 if cents_key == "cash_cents" else None

    def _rebuild_report_artifacts(self) -> int:
        """从 report artifact 事件重建 report_artifacts；无事件时保持空表。"""
        self._conn.execute("DELETE FROM report_artifacts")

        count = 0
        for ev in self._all_events():
            event_type = ev.get("event_type", "")
            if event_type not in {"report.artifact.created", "report.generated", "artifact.created"}:
                continue

            payload = ev["payload"]
            content = payload.get("content")
            report_type = payload.get("report_type") or payload.get("type")
            if content is None or not report_type:
                continue

            metadata = ev.get("metadata", {})
            self._conn.execute(
                """INSERT OR REPLACE INTO report_artifacts
                   (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    payload.get("artifact_id", ev.get("event_id")),
                    payload.get("run_id", metadata.get("run_id", "")),
                    report_type,
                    payload.get("format", payload.get("fmt", "text")),
                    content,
                    payload.get("delivered_to", ""),
                    payload.get("created_at", ev.get("occurred_at", "")),
                ),
            )
            count += 1

        return count
