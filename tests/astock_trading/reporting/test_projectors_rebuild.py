"""Focused rebuild coverage for reporting projectors."""

from astock_trading.platform.events import EventStore
from astock_trading.reporting.projectors import ProjectionUpdater


def _db(mysql_conn):
    conn = mysql_conn
    return conn


def test_rebuild_candidate_pool_from_event_log_after_projection_delete(mysql_conn):
    conn = _db(mysql_conn)
    try:
        store = EventStore(conn)
        updater = ProjectionUpdater(store, conn)

        store.append(
            stream="candidate:001",
            stream_type="candidate",
            event_type="candidate.added",
            payload={"code": "001", "name": "A", "pool_tier": "watch", "score": 6.5},
        )
        store.append(
            stream="candidate:001",
            stream_type="candidate",
            event_type="candidate.promoted",
            payload={"code": "001", "name": "A", "pool_tier": "core", "score": 7.8, "note": "manual"},
        )
        store.append(
            stream="strategy:001",
            stream_type="strategy",
            event_type="pool.demoted",
            payload={"code": "001", "name": "A", "from": "core", "to": "watch", "reason": "weak_score"},
        )
        store.append(
            stream="candidate:002",
            stream_type="candidate",
            event_type="candidate.added",
            payload={"code": "002", "name": "B", "pool_tier": "watch", "score": 6.1},
        )
        store.append(
            stream="strategy:002",
            stream_type="strategy",
            event_type="pool.removed",
            payload={"code": "002", "name": "B", "from": "watch", "score": 5.0},
        )
        store.append(
            stream="candidate:003",
            stream_type="candidate",
            event_type="candidate.added",
            payload={"code": "003", "name": "C", "pool_tier": "watch", "score": 6.0},
        )
        store.append(
            stream="candidate:003",
            stream_type="candidate",
            event_type="candidate.rejected",
            payload={"code": "003", "reason": "manual"},
        )
        store.append(
            stream="candidate:004",
            stream_type="candidate",
            event_type="candidate.added",
            payload={"code": "004", "name": "D", "pool_tier": "watch", "score": 5.1},
        )
        store.append(
            stream="candidate:004",
            stream_type="candidate",
            event_type="candidate.updated",
            payload={
                "code": "004",
                "name": "D",
                "pool_tier": "watch",
                "score": 5.3,
                "note": "refresh",
            },
        )

        conn.execute("DELETE FROM projection_candidate_pool")

        stats = updater.rebuild_all()

        rows = conn.execute(
            "SELECT code, pool_tier, name, score, note FROM projection_candidate_pool ORDER BY code"
        ).fetchall()
        assert stats["candidate_pool"] == 2
        assert [dict(row) for row in rows] == [
            {"code": "001", "pool_tier": "watch", "name": "A", "score": 7.8, "note": "weak_score"},
            {"code": "004", "pool_tier": "watch", "name": "D", "score": 5.3, "note": "refresh"},
        ]
    finally:
        conn.close()


def test_rebuild_balances_from_balance_events(mysql_conn):
    conn = _db(mysql_conn)
    try:
        store = EventStore(conn)
        store.append(
            stream="balance:main",
            stream_type="balance",
            event_type="balance.updated",
            payload={
                "scope": "main",
                "cash_cents": 1234500,
                "total_asset_cents": 2345600,
                "weekly_buy_count": 2,
                "daily_pnl_cents": -1200,
                "consecutive_loss_days": 1,
            },
        )

        stats = ProjectionUpdater(store, conn).rebuild_all()

        row = conn.execute("SELECT * FROM projection_balances WHERE scope = 'main'").fetchone()
        assert stats["balances"] == 1
        assert dict(row) | {"updated_at": "ignored"} == {
            "scope": "main",
            "cash_cents": 1234500,
            "total_asset_cents": 2345600,
            "weekly_buy_count": 2,
            "daily_pnl_cents": -1200,
            "consecutive_loss_days": 1,
            "updated_at": "ignored",
        }
    finally:
        conn.close()


def test_rebuild_all_clears_all_projection_tables_before_replay(mysql_conn):
    conn = _db(mysql_conn)
    try:
        store = EventStore(conn)
        conn.execute(
            """INSERT INTO projection_positions
               (code, name, style, shares, avg_cost_cents, entry_date, entry_day_low_cents,
                highest_since_entry_cents, current_price_cents, updated_at)
               VALUES ('STALE_POS', 'stale', 'x', 1, 1, '2026-01-01', 1, 1, 1, '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO projection_orders
               (order_id, code, side, shares, price_cents, status, broker, created_at, filled_at, updated_at)
               VALUES ('STALE_ORDER', 'STALE', 'buy', 1, 1, 'pending', '', '2026-01-01', NULL, '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO projection_balances
               (scope, cash_cents, total_asset_cents, weekly_buy_count, daily_pnl_cents,
                consecutive_loss_days, updated_at)
               VALUES ('stale', 1, 1, 0, 0, 0, '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES ('STALE_POOL', 'watch', 'stale', 1.0, '2026-01-01', '2026-01-01', 0, '')"""
        )
        conn.execute(
            """INSERT INTO projection_market_state
               (index_symbol, name, signal, price_cents, change_pct, ma20_pct, ma60_pct, updated_at)
               VALUES ('STALE_MARKET', 'stale', 'RED', 1, 0, 0, 0, '2026-01-01')"""
        )
        conn.execute(
            """INSERT INTO report_artifacts
               (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
               VALUES ('STALE_REPORT', 'run', 'daily', 'text', 'stale', '', '2026-01-01')"""
        )

        stats = ProjectionUpdater(store, conn).rebuild_all()

        assert stats == {
            "positions": 0,
            "orders": 0,
            "balances": 0,
            "candidate_pool": 0,
            "market_state": 0,
            "report_artifacts": 0,
        }
        for table in (
            "projection_positions",
            "projection_orders",
            "projection_balances",
            "projection_candidate_pool",
            "projection_market_state",
            "report_artifacts",
        ):
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    finally:
        conn.close()
