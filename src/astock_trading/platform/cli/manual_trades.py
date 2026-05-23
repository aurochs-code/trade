"""Manual trade confirmation commands."""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore
from astock_trading.platform.manual_trade_state import (
    actionable_pending_manual_trades,
    load_manual_confirmation_policy,
    manual_trade_states,
    stale_pending_manual_trades,
)
from astock_trading.platform.time import utc_now_iso


manual_trades_app = typer.Typer(name="manual-trades", help="人工确认单")


def _manual_trade_state(events: list[dict]) -> list[dict]:
    return manual_trade_states(events)


def _filter_manual_trade_states(states: list[dict], status: str) -> list[dict]:
    if status == "all":
        return states
    if status == "pending":
        return actionable_pending_manual_trades(states)
    if status == "stale":
        return stale_pending_manual_trades(states)
    return [item for item in states if item.get("status") == status]


@manual_trades_app.command("list")
def manual_trades_list(
    status: str = typer.Option("pending", "--status", help="pending / stale / confirmed / rejected / expired / all"),
    limit: int = typer.Option(100, help="最大事件条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """列出人工确认单。"""
    conn = connect()
    try:
        store = EventStore(conn)
        states = _manual_trade_state(store.query(stream_type="manual_trade", limit=limit))
        states = _filter_manual_trade_states(states, status)
        if as_json:
            json_or_text(states, True)
        else:
            if not states:
                typer.echo("无人工确认单")
            for item in states:
                stale_note = f" {item.get('stale_reason_label')}" if item.get("stale") else ""
                typer.echo(
                    f"{item.get('status')} {item.get('side')} "
                    f"{item.get('code')} {item.get('name', '')} "
                    f"score={item.get('score', '-')}{stale_note}"
                )
    finally:
        conn.close()


@manual_trades_app.command("expire-stale")
def manual_trades_expire_stale(
    max_age_hours: int = typer.Option(0, "--max-age-hours", help="覆盖人工确认最大有效小时数；0 表示使用配置"),
    yes: bool = typer.Option(False, "--yes", help="确认写入过期事件"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """把已过期或错过买入窗口的人工确认单标记为已过期。"""
    conn = connect()
    try:
        store = EventStore(conn)
        policy = load_manual_confirmation_policy()
        if max_age_hours > 0:
            policy["pending_max_age_hours"] = max_age_hours
        states = manual_trade_states(
            store.query(stream_type="manual_trade", limit=500),
            policy=policy,
        )
        stale_items = stale_pending_manual_trades(states)
        if not yes:
            payload = {
                "command": "manual-trades expire-stale",
                "status": "needs_confirmation",
                "expired_count": 0,
                "candidates_count": len(stale_items),
                "candidates": stale_items,
                "next_action": "atrade manual-trades expire-stale --yes --json",
            }
            json_or_text(payload, as_json)
            return

        expired: list[dict] = []
        for item in stale_items:
            event_payload = {
                "status": "expired",
                "code": item.get("code", ""),
                "name": item.get("name", ""),
                "side": item.get("side", ""),
                "reason": "stale_confirmation",
                "stale_reason": item.get("stale_reason", ""),
                "stale_reason_label": item.get("stale_reason_label", ""),
                "requested_event_id": item.get("requested_event_id", ""),
                "requested_at": item.get("requested_at", ""),
                "expired_at": utc_now_iso(),
                "max_age_hours": item.get("max_age_hours", policy["pending_max_age_hours"]),
            }
            event_id = store.append(
                stream=str(item.get("stream") or f"manual_trade:{item.get('code', '')}"),
                stream_type="manual_trade",
                event_type="manual_trade.expired",
                payload=event_payload,
                metadata={"execution": "manual", "reason": "stale_confirmation"},
            )
            expired.append({**item, "expiration_event_id": event_id})
        payload = {
            "command": "manual-trades expire-stale",
            "status": "success",
            "expired_count": len(expired),
            "expired": expired,
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()


@manual_trades_app.command("confirm")
def manual_trades_confirm(
    code: str = typer.Argument(..., help="股票代码"),
    order_id: str = typer.Option("", "--order-id", help="关联的手工成交订单 ID"),
    note: str = typer.Option("", "--note", help="确认备注"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """标记人工确认单为已确认。"""
    conn = connect()
    try:
        store = EventStore(conn)
        event_id = store.append(
            stream=f"manual_trade:{code}",
            stream_type="manual_trade",
            event_type="manual_trade.confirmed",
            payload={"status": "confirmed", "code": code, "order_id": order_id, "note": note},
            metadata={"execution": "manual"},
        )
        json_or_text({"status": "confirmed", "event_id": event_id, "code": code}, as_json)
    finally:
        conn.close()


@manual_trades_app.command("reject")
def manual_trades_reject(
    code: str = typer.Argument(..., help="股票代码"),
    reason: str = typer.Option("", "--reason", help="拒绝原因"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """标记人工确认单为已拒绝。"""
    conn = connect()
    try:
        store = EventStore(conn)
        event_id = store.append(
            stream=f"manual_trade:{code}",
            stream_type="manual_trade",
            event_type="manual_trade.rejected",
            payload={"status": "rejected", "code": code, "reason": reason},
            metadata={"execution": "manual"},
        )
        json_or_text({"status": "rejected", "event_id": event_id, "code": code}, as_json)
    finally:
        conn.close()
