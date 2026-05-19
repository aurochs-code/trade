"""Dashboard read model.

为 Web / 手机展示提供稳定 JSON 数据契约；不承载交易写操作。
"""

from __future__ import annotations

from typing import Any

from astock_trading.platform.domain_events import MANUAL_TRADE_REQUESTED
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import utc_now_iso


def build_dashboard_snapshot(conn: Any) -> dict:
    """汇总 dashboard 首屏所需的只读状态。"""
    portfolio = _portfolio(conn)
    candidate_pool = _candidate_pool(conn)
    manual_trades = _manual_trades(conn)
    market = _market(conn)
    runs = _runs(conn)
    reports = _reports(conn)
    non_empty = any([
        portfolio["position_count"],
        portfolio["balance"].get("total_asset_cents", 0),
        sum(candidate_pool["counts"].values()),
        manual_trades["pending_count"],
        market["states"],
        runs["latest"],
        reports["latest"],
    ])
    return {
        "analysis": "dashboard_snapshot",
        "status": "ok" if non_empty else "empty",
        "generated_at": utc_now_iso(),
        "portfolio": portfolio,
        "candidate_pool": candidate_pool,
        "manual_trades": manual_trades,
        "market": market,
        "runs": runs,
        "reports": reports,
        "guardrails": {
            "read_only": True,
            "trading_actions_enabled": False,
            "manual_confirmation_required": True,
            "reason": "Dashboard snapshot 只用于展示，不提供下单、撤单、改配置或确认交易能力。",
        },
    }


def _portfolio(conn: Any) -> dict:
    balance_row = conn.execute(
        """SELECT scope, cash_cents, total_asset_cents, weekly_buy_count, daily_pnl_cents,
                  consecutive_loss_days, updated_at
           FROM projection_balances
           WHERE scope = 'main'
           LIMIT 1"""
    ).fetchone()
    positions = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT code, name, style, shares, avg_cost_cents, entry_date,
                      current_price_cents, unrealized_pnl_cents, currency, updated_at
               FROM projection_positions
               ORDER BY entry_date, code
               LIMIT 50"""
        ).fetchall()
    ]
    total_market_cents = sum(
        int((row.get("current_price_cents") or row.get("avg_cost_cents") or 0) or 0)
        * int(row.get("shares") or 0)
        for row in positions
    )
    balance = _row_dict(balance_row) if balance_row else {}
    total_asset_cents = int(balance.get("total_asset_cents") or 0)
    return {
        "balance": balance,
        "position_count": len(positions),
        "total_market_cents": total_market_cents,
        "exposure_pct": round(total_market_cents / total_asset_cents, 4) if total_asset_cents > 0 else 0.0,
        "positions": positions,
    }


def _candidate_pool(conn: Any) -> dict:
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
               FROM projection_candidate_pool
               ORDER BY pool_tier, score DESC, code
               LIMIT 100"""
        ).fetchall()
    ]
    counts: dict[str, int] = {}
    for row in rows:
        tier = str(row.get("pool_tier") or "unknown")
        counts[tier] = counts.get(tier, 0) + 1
    return {
        "counts": counts,
        "top": rows[:20],
    }


def _manual_trades(conn: Any) -> dict:
    events = EventStore(conn).query(event_type=MANUAL_TRADE_REQUESTED, limit=200)
    pending = []
    for event in reversed(events):
        payload = event.get("payload") or {}
        if str(payload.get("status") or "pending") != "pending":
            continue
        pending.append({
            "event_id": event.get("event_id"),
            "occurred_at": event.get("occurred_at"),
            "code": payload.get("code", ""),
            "name": payload.get("name", payload.get("code", "")),
            "side": payload.get("side", ""),
            "score": payload.get("score", 0),
            "source_event_id": payload.get("source_event_id", ""),
        })
        if len(pending) >= 20:
            break
    return {
        "pending_count": len(pending),
        "pending_items": pending,
    }


def _market(conn: Any) -> dict:
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT index_symbol, name, `signal`, price_cents, change_pct, ma20_pct, ma60_pct, updated_at
               FROM projection_market_state
               ORDER BY index_symbol
               LIMIT 20"""
        ).fetchall()
    ]
    return {"states": rows}


def _runs(conn: Any) -> dict:
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT run_id, run_type, scope, config_version, status, started_at, finished_at, error_message
               FROM run_log
               ORDER BY started_at DESC
               LIMIT 20"""
        ).fetchall()
    ]
    return {"latest": rows}


def _reports(conn: Any) -> dict:
    rows = [
        _row_dict(row)
        for row in conn.execute(
            """SELECT artifact_id, run_id, report_type, format, delivered_to, created_at
               FROM report_artifacts
               ORDER BY created_at DESC
               LIMIT 20"""
        ).fetchall()
    ]
    return {"latest": rows}


def _row_dict(row: Any) -> dict:
    if not row:
        return {}
    return {key: row[key] for key in row.keys()}
