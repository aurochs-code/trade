"""Dashboard snapshot data contract tests."""

from __future__ import annotations

from astock_trading.platform.dashboard import build_dashboard_snapshot
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore


def test_dashboard_snapshot_summarizes_operational_state(tmp_path):
    db_path = tmp_path / "dashboard.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        conn.execute(
            """INSERT INTO projection_balances
               (scope, cash_cents, total_asset_cents, weekly_buy_count, daily_pnl_cents,
                consecutive_loss_days, updated_at)
               VALUES ('main', 6000000, 10000000, 1, 12000, 0, '2026-05-19T09:30:00+08:00')"""
        )
        conn.execute(
            """INSERT INTO projection_positions
               (code, name, style, shares, avg_cost_cents, entry_date, current_price_cents,
                unrealized_pnl_cents, currency, updated_at)
               VALUES ('600703', '三安光电', 'momentum', 100, 1000, '2026-05-18', 1100,
                       10000, 'CNY', '2026-05-19T09:30:00+08:00')"""
        )
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES ('002138', 'core', '双环传动', 6.8, '2026-05-18', '2026-05-19', 2, '观察')"""
        )
        conn.execute(
            """INSERT INTO projection_market_state
               (index_symbol, name, signal, price_cents, change_pct, updated_at)
               VALUES ('000001', '上证指数', 'YELLOW', 310000, 0.8, '2026-05-19T09:30:00+08:00')"""
        )
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at)
               VALUES ('run_morning', 'morning', 'cn_a', 'v1', 'completed',
                       '2026-05-19T01:00:00+00:00', '2026-05-19T01:01:00+00:00')"""
        )
        conn.execute(
            """INSERT INTO report_artifacts
               (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
               VALUES ('report_1', 'run_morning', 'morning', 'markdown', '# 早报',
                       'local', '2026-05-19T01:02:00+00:00')"""
        )
        store.append(
            "manual_trade:002138",
            "manual_trade",
            "manual_trade.requested",
            {"code": "002138", "name": "双环传动", "status": "pending", "side": "buy", "score": 6.8},
        )

        payload = build_dashboard_snapshot(conn)
    finally:
        conn.close()

    assert payload["analysis"] == "dashboard_snapshot"
    assert payload["portfolio"]["balance"]["total_asset_cents"] == 10000000
    assert payload["portfolio"]["position_count"] == 1
    assert payload["candidate_pool"]["counts"]["core"] == 1
    assert payload["manual_trades"]["pending_count"] == 1
    assert payload["market"]["states"][0]["signal"] == "YELLOW"
    assert payload["runs"]["latest"][0]["run_type"] == "morning"
    assert payload["reports"]["latest"][0]["report_type"] == "morning"
    assert payload["guardrails"]["read_only"] is True
    assert payload["guardrails"]["trading_actions_enabled"] is False


def test_dashboard_snapshot_empty_db_still_returns_sections(tmp_path):
    db_path = tmp_path / "empty_dashboard.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        payload = build_dashboard_snapshot(conn)
    finally:
        conn.close()

    assert payload["status"] == "empty"
    assert payload["portfolio"]["position_count"] == 0
    assert payload["manual_trades"]["pending_count"] == 0
