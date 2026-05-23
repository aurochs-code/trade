"""历史证据回填。

旧事件不能事后改写，也不能伪造成当时已经存在的原始分析。
本模块只追加 append-only 回填事件，并把可恢复的旧 payload 原样挂到证据链上。
"""

from __future__ import annotations

from typing import Any

from astock_trading.platform.domain_events import (
    EVIDENCE_BACKFILLED,
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_OUTCOME_RECORDED,
)
from astock_trading.platform.events import EventStore

STRATEGY_EVENTS_FOR_BACKFILL = {"score.calculated", "decision.suggested", "manual_trade.requested"}
TRADE_EVENTS_FOR_BACKFILL = {"order.created", "order.filled"}
SIGNAL_EVIDENCE_FIELDS = (
    "entry_signal",
    "primary_strategy_route",
    "primary_strategy_route_label",
    "strategy_routes",
    "technical_detail",
    "data_quality",
)


def backfill_legacy_evidence(
    conn: Any,
    *,
    code: str = "",
    apply: bool = False,
    limit: int = 5000,
) -> dict:
    """为旧事件追加可追溯证据，不改写历史事件。"""
    store = EventStore(conn)
    events = _query_backfillable_events(conn, code=code, limit=limit)
    existing_sources = _existing_source_event_ids(conn, limit=limit)
    existing_trade_events = _existing_trade_event_types(conn, limit=limit)
    order_created_by_stream = {
        event["stream"]: event
        for event in events
        if event.get("event_type") == "order.created"
    }

    created: list[dict] = []
    planned: list[dict] = []

    for event in events:
        event_type = event.get("event_type", "")
        if event["event_id"] in existing_sources:
            continue
        if event_type in STRATEGY_EVENTS_FOR_BACKFILL:
            item = _signal_evidence_backfill_item(conn, event)
            if not item and _strategy_event_needs_backfill(event):
                item = _strategy_backfill_item(event)
        elif event_type == "order.created":
            if TRADE_HYPOTHESIS_RECORDED in existing_trade_events.get(_trade_stream_for_order_event(event), set()):
                continue
            item = _trade_hypothesis_backfill_item(event)
        elif event_type == "order.filled":
            if TRADE_OUTCOME_RECORDED in existing_trade_events.get(_trade_stream_for_order_event(event), set()):
                continue
            item = _trade_outcome_backfill_item(event, order_created_by_stream.get(event["stream"]))
        else:
            continue

        if not item:
            continue
        planned.append(item)
        if apply:
            event_id = store.append(
                stream=item["stream"],
                stream_type=item["stream_type"],
                event_type=item["event_type"],
                payload=item["payload"],
                metadata=item["metadata"],
            )
            created.append({
                "event_id": event_id,
                "event_type": item["event_type"],
                "stream": item["stream"],
                "source_event_id": item["payload"].get("source_event_id", ""),
            })

    return {
        "status": "applied" if apply else "dry_run",
        "apply": apply,
        "code": code,
        "scanned_count": len(events),
        "planned_count": len(planned),
        "created_count": len(created),
        "planned": _public_items(planned),
        "created": created,
    }


def _query_backfillable_events(conn: Any, *, code: str, limit: int) -> list[dict]:
    params: list[Any] = []
    code_filter = ""
    if code:
        code_filter = " AND (stream LIKE ? OR json_extract(payload_json, '$.code') = ?)"
        params.extend([f"%:{code}%", code])
    params.append(limit)
    rows = conn.execute(
        f"""SELECT * FROM event_log
            WHERE event_type IN (
                'score.calculated',
                'decision.suggested',
                'manual_trade.requested',
                'order.created',
                'order.filled'
            ){code_filter}
            ORDER BY occurred_at, stream_version
            LIMIT ?""",
        tuple(params),
    ).fetchall()
    return [EventStore._row_to_dict(row) for row in rows]


def _existing_source_event_ids(conn: Any, *, limit: int) -> set[str]:
    rows = conn.execute(
        """SELECT payload_json FROM event_log
           WHERE event_type IN (?, ?, ?)
           ORDER BY occurred_at, stream_version
           LIMIT ?""",
        (EVIDENCE_BACKFILLED, TRADE_HYPOTHESIS_RECORDED, TRADE_OUTCOME_RECORDED, limit),
    ).fetchall()
    ids: set[str] = set()
    for row in rows:
        payload = EventStore._row_to_dict({"payload_json": row["payload_json"], "metadata_json": "{}"})["payload"]
        source_event_id = str(payload.get("source_event_id") or "")
        if source_event_id:
            ids.add(source_event_id)
    return ids


def _existing_trade_event_types(conn: Any, *, limit: int) -> dict[str, set[str]]:
    rows = conn.execute(
        """SELECT stream, event_type FROM event_log
           WHERE event_type IN (?, ?)
           ORDER BY occurred_at, stream_version
           LIMIT ?""",
        (TRADE_HYPOTHESIS_RECORDED, TRADE_OUTCOME_RECORDED, limit),
    ).fetchall()
    result: dict[str, set[str]] = {}
    for row in rows:
        result.setdefault(row["stream"], set()).add(row["event_type"])
    return result


def _strategy_event_needs_backfill(event: dict) -> bool:
    payload = event.get("payload", {}) or {}
    if event.get("event_type") == "score.calculated":
        return not payload.get("dimensions") or not payload.get("source_observation_id")
    if event.get("event_type") == "decision.suggested":
        return (
            not payload.get("decision_inputs")
            or not payload.get("source_score_event_id")
            or not payload.get("decision_rules")
        )
    return False


def _signal_evidence_backfill_item(conn: Any, event: dict) -> dict:
    if event.get("event_type") not in {"decision.suggested", "manual_trade.requested"}:
        return {}
    payload = event.get("payload", {}) or {}
    if not _is_buy_signal_event(event):
        return {}
    missing_fields = [field for field in SIGNAL_EVIDENCE_FIELDS if not _has_signal_field(payload, field)]
    if not missing_fields:
        return {}

    source_score_event_id = str(payload.get("source_score_event_id") or "")
    if not source_score_event_id:
        return {}
    score_event = _event_by_id(conn, source_score_event_id)
    if not score_event:
        return {}
    score_payload = score_event.get("payload", {}) or {}
    recovered = _recovered_signal_evidence(score_payload)
    if not any(_has_signal_field(recovered, field) for field in SIGNAL_EVIDENCE_FIELDS):
        return {}

    code = _event_code(event)
    return {
        "stream": f"evidence:{code or 'unknown'}",
        "stream_type": "evidence",
        "event_type": EVIDENCE_BACKFILLED,
        "payload": {
            "code": code,
            "source_event_id": event["event_id"],
            "source_event_type": event.get("event_type", ""),
            "source_stream": event.get("stream", ""),
            "source_score_event_id": source_score_event_id,
            "evidence_status": "recovered_signal_evidence",
            "missing_fields": missing_fields,
            "recovered_evidence": recovered,
            "legacy_payload": payload,
            "backfill_note": (
                "历史买入意向缺少入场路线字段；已从当时引用的 score.calculated "
                "追加可审计证据事件，未改写原始事件。"
            ),
        },
        "metadata": {"source": "legacy_evidence_backfill"},
    }


def _is_buy_signal_event(event: dict) -> bool:
    payload = event.get("payload", {}) or {}
    if event.get("event_type") == "decision.suggested":
        return str(payload.get("action") or "").upper() == "BUY"
    if event.get("event_type") == "manual_trade.requested":
        return str(payload.get("side") or "").lower() == "buy"
    return False


def _event_by_id(conn: Any, event_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM event_log WHERE event_id = ? LIMIT 1", (event_id,)).fetchone()
    return EventStore._row_to_dict(row) if row else None


def _recovered_signal_evidence(score_payload: dict) -> dict:
    routes = score_payload.get("strategy_routes") or []
    primary_route = score_payload.get("primary_strategy_route")
    return {
        "entry_signal": bool(score_payload.get("entry_signal", False)),
        "primary_strategy_route": primary_route,
        "primary_strategy_route_label": _primary_route_label(routes, primary_route),
        "strategy_routes": routes,
        "technical_detail": score_payload.get("technical_detail", ""),
        "data_quality": score_payload.get("data_quality", ""),
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


def _has_signal_field(payload: dict, field: str) -> bool:
    if field == "entry_signal":
        return payload.get(field) is not None
    value = payload.get(field)
    return bool(value)


def _strategy_backfill_item(event: dict) -> dict:
    payload = event.get("payload", {}) or {}
    code = _event_code(event)
    missing_fields = []
    if event.get("event_type") == "score.calculated":
        for field in ("dimensions", "source_observation_id"):
            if not payload.get(field):
                missing_fields.append(field)
    elif event.get("event_type") == "decision.suggested":
        for field in ("decision_inputs", "source_score_event_id", "decision_rules"):
            if not payload.get(field):
                missing_fields.append(field)
    return {
        "stream": f"evidence:{code or 'unknown'}",
        "stream_type": "evidence",
        "event_type": EVIDENCE_BACKFILLED,
        "payload": {
            "code": code,
            "source_event_id": event["event_id"],
            "source_event_type": event.get("event_type", ""),
            "source_stream": event.get("stream", ""),
            "evidence_status": "legacy_partial",
            "missing_fields": missing_fields,
            "legacy_payload": payload,
            "backfill_note": "历史事件缺少新证据字段；仅保留旧 payload，不补写当时不存在的原始分析。",
        },
        "metadata": {"source": "legacy_evidence_backfill"},
    }


def _trade_hypothesis_backfill_item(event: dict) -> dict:
    payload = event.get("payload", {}) or {}
    order_id = str(payload.get("order_id") or _order_id_from_stream(event.get("stream", "")))
    code = _event_code(event)
    if not order_id or not code:
        return {}
    reason = str(payload.get("reason") or payload.get("broker") or "legacy_manual_trade")
    return {
        "stream": f"trade:{code}:{order_id}",
        "stream_type": "trade",
        "event_type": TRADE_HYPOTHESIS_RECORDED,
        "payload": {
            "order_id": order_id,
            "code": code,
            "name": payload.get("name") or code,
            "side": payload.get("side", ""),
            "shares": payload.get("shares", 0),
            "price_cents": payload.get("price_cents", 0),
            "fee_cents": payload.get("fee_cents", 0),
            "source_event_id": event["event_id"],
            "source_score_event_id": payload.get("source_score_event_id", ""),
            "hypothesis": {
                "thesis": "历史成交回填：原始交易前假设缺失，不能事后补写为确定理由。",
                "manual_reason": reason,
                "backfill_status": "legacy_partial",
                "missing_original_hypothesis": True,
            },
        },
        "metadata": {"source": "legacy_evidence_backfill", "execution": "manual"},
    }


def _trade_outcome_backfill_item(event: dict, created_event: dict | None) -> dict:
    payload = event.get("payload", {}) or {}
    created_payload = (created_event or {}).get("payload", {}) or {}
    order_id = str(payload.get("order_id") or _order_id_from_stream(event.get("stream", "")))
    code = _event_code(event)
    if not order_id or not code:
        return {}
    return {
        "stream": f"trade:{code}:{order_id}",
        "stream_type": "trade",
        "event_type": TRADE_OUTCOME_RECORDED,
        "payload": {
            "order_id": order_id,
            "code": code,
            "name": created_payload.get("name") or payload.get("name") or code,
            "side": payload.get("side") or created_payload.get("side", ""),
            "status": "filled",
            "shares": payload.get("shares") or created_payload.get("shares", 0),
            "fill_price_cents": payload.get("fill_price_cents") or payload.get("price_cents", 0),
            "fee_cents": payload.get("fee_cents", 0),
            "reason": payload.get("reason") or "legacy_backfill",
            "source_event_id": event["event_id"],
            "source_score_event_id": payload.get("source_score_event_id", ""),
            "position_after": None,
            "backfill_status": "legacy_partial",
        },
        "metadata": {"source": "legacy_evidence_backfill", "execution": "manual"},
    }


def _event_code(event: dict) -> str:
    payload = event.get("payload", {}) or {}
    code = str(payload.get("code") or "").strip()
    if code:
        return code
    stream = str(event.get("stream") or "")
    parts = stream.split(":")
    return parts[1] if len(parts) >= 2 else ""


def _order_id_from_stream(stream: str) -> str:
    parts = stream.split(":")
    return parts[2] if len(parts) >= 3 else ""


def _trade_stream_for_order_event(event: dict) -> str:
    code = _event_code(event)
    order_id = str((event.get("payload", {}) or {}).get("order_id") or _order_id_from_stream(event.get("stream", "")))
    return f"trade:{code}:{order_id}" if code and order_id else ""


def _public_items(items: list[dict]) -> list[dict]:
    return [
        {
            "event_type": item["event_type"],
            "stream": item["stream"],
            "source_event_id": item["payload"].get("source_event_id", ""),
        }
        for item in items
    ]
